use necronomicon::{ByteStr, SharedImpl};
use net::session::SessionWriter;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ChainRole {
    Candidate,
    Head,
    HeadAndTail,
    Middle,
    Resigner,
    Tail,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ConnectionState {
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
pub(crate) struct Backend {
    pub(crate) addr: ByteStr<SharedImpl>,
    pub(crate) role: ChainRole,
    pub(crate) session: SessionWriter,
    pub(crate) successor_connection: ConnectionState,
}

#[derive(Clone, Debug)]
pub(crate) struct Frontend {
    pub(crate) addr: ByteStr<SharedImpl>,
    pub(crate) session: SessionWriter,
}
