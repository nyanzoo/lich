#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("io error {0}")]
    IO(#[from] std::io::Error),

    #[error("necronomicon error {0}")]
    Necronomicon(#[from] necronomicon::Error),
}
