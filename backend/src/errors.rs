use thiserror::Error;

#[derive(uniffi::Error, Error, Debug)]
pub enum P2PError {
    #[error("Failed to bootstrap and connect to the Tor network: {0}")]
    TorConnectionError(String),

    #[error("Failed to initialize or connect to the onion service: {0}")]
    OnionConnectionError(String),

    #[error("Failed to send message over the WebSocket stream: {0}")]
    MessageSendError(String),

    #[error("Chat connection not found for address: {0}")]
    ChatNotFoundError(String),
}
