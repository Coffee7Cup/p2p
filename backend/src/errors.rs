use thiserror::Error;

#[derive(Error, Debug)]
pub enum P2PError {
    #[error("Error While Connecting to TOR")]
    TorConnectioError,

    #[error("Backend WorkQueue is full")]
    QueueBufferOverflowError,

    #[error("Error while connecting to onion address")]
    OnionConnectionError,
}
