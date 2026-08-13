use std::collections::HashMap;

use crate::tor::Client;

use std::sync::Arc;
use tokio::sync::{Mutex, mpsc};
use tokio_tungstenite::tungstenite::Message;

pub type Chats = Arc<Mutex<HashMap<String, mpsc::Sender<Message>>>>;

struct AppManager {
    client: Client,
    chats: Chats,
}

impl AppManager {
    pub fn new(tor_client: Client) -> Self {
        let mut hash: HashMap<String, mpsc::Sender<Message>> = HashMap::new();
        let mut chats = Arc::new(Mutex::new(hash));
        Self {
            client: tor_client,
            chats: chats,
        }
    }

    pub fn connect_to_onion_address(onion_addr: &str) {}
}
