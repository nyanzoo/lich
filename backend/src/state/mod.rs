use std::time::Duration;

pub(crate) mod init;
pub(crate) mod ready;
pub(crate) mod transfer;
pub(crate) mod update;
pub(crate) mod waiting_for_operator;

const CHANNEL_CAPACITY: usize = 1024;
const TIMEOUT: Duration = Duration::from_secs(35);

pub trait State {
    fn next(self: Box<Self>) -> Box<dyn State>;
}
