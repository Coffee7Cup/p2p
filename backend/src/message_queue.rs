// ===================================================
// On Frontend do
// ===================================================

// class Receiver : MsgReceiver {
//     var channel = Channel<Message>(Channel.UNLIMITED)
//
//     override fun msgFromRust(msg: MsgBackend) {
//         channel.trySend(msg)
//     }
// }

// ---------------------------------------------------
// Now you can create an instance of the class and use
// it to send msgs to the backend - but first initialze the bridge
// ---------------------------------------------------

// val receiver = Receiver()
// val bridge = P2PBridge(receiver)
//
// ---------------------------------------------------
// Now you can use
// ---------------------------------------------------

// bridge.send_to_backend(FEmsg)

// Remember that FEmsg should be of type MsgFrontend
// ===================================================

// TODO: Add the uniffi macros

use std::sync::Arc;

use futures::{SinkExt, channel::mpsc};
use tokio_tungstenite::tungstenite::Message;

use crate::Result;

use crate::tor::Client;

pub enum TorStatus {
    Online,
    Offline,
    Connecting,
}

pub enum BackendMsg {
    ChatMsg(String),
    Error(String),
    TorStatus(TorStatus),
}

impl From<Message> for BackendMsg {
    fn from(msg: Message) -> Self {
        match msg {
            Message::Text(text) => Self::ChatMsg(text.to_string()),
            _ => Self::Error("Cannot read the message".to_string()),
        }
    }
}

pub enum FrontendMsg {
    MsgToChat((String, String)),
}

trait MsgReceiver: Send + Sync {
    fn msg_from_rust(&self, msg: BackendMsg);
}

pub struct P2PBridge {
    rs_tx: mpsc::UnboundedSender<FrontendMsg>,
    sender: Arc<dyn MsgReceiver>,
}

impl P2PBridge {
    fn new(receiver: Arc<dyn MsgReceiver>) -> Arc<Self> {
        let (tx, mut rx) = mpsc::unbounded::<FrontendMsg>();
        let bridge = Arc::new(Self {
            rs_tx: tx,
            sender: receiver,
        });

        tokio::spawn(async move {
            while let Ok(msg) = rx.recv().await {
                // TODO: complete this code
            }
        });

        bridge
    }

    //this for frontedn to use
    fn send_to_backend(&mut self, msg: FrontendMsg) {
        self.rs_tx.send(msg);
    }

    pub fn send_to_frontend(&self, msg: BackendMsg) {
        self.sender.msg_from_rust(msg);
    }
}
// i guess this will be droped out of memory, I might have to return the Client object
async fn init(state_dir: String, cache_dir: String, bridge: Arc<P2PBridge>) -> Result<Arc<Client>> {
    Client::new(state_dir, cache_dir, bridge).await
}
