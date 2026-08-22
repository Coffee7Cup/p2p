uniffi::setup_scaffolding!();

mod errors;
mod tor;

pub use errors::P2PError;
pub type Result<T> = std::result::Result<T, P2PError>;

pub use tor::{BackendMsg, Client, MsgReceiver, TorStatus};

#[uniffi::export]
pub fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .try_init();
}
