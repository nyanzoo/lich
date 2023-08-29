#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("io error {0}")]
    IO(#[from] std::io::Error),

    #[error("necronomicon error {0}")]
    Necronomicon(#[from] necronomicon::Error),

    #[error("phylactery error {0}")]
    Phylactery(#[from] phylactery::Error),

    #[error("queue {0} does not exist")]
    QueueDoesNotExist(String),
}
