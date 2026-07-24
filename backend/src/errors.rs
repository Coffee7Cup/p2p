use thiserror::Error;

#[derive(Error, Debug)]
pub enum PTPError {
    #[error("Error While Connecting to TOR")]
    TorConnectioError,

    #[error("Backend WorkQueue is full")]
    QueueBufferOverflowError,
}
