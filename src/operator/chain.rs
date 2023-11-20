use crate::common::session::SessionWriter;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChainRole {
    Candidate,
    Head,
    HeadAndTail,
    Middle,
    Resigner,
    Tail,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConnectionState {
    Connected,
    Disconnected,
}

impl From<bool> for ConnectionState {
    fn from(disconnected: bool) -> Self {
        if disconnected {
            ConnectionState::Disconnected
        } else {
            ConnectionState::Connected
        }
    }
}

#[derive(Clone, Debug)]
pub struct Backend {
    pub addr: String,
    pub role: ChainRole,
    pub session: SessionWriter,
    pub successor_connection: ConnectionState,
}

#[derive(Clone, Debug)]
pub struct Frontend {
    pub addr: String,
    pub session: SessionWriter,
}
