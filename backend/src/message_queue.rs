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

use std::sync::Arc;

use futures::{SinkExt, channel::mpsc};

enum MsgBackend {
    Msg(String),
}

enum MsgFrontend {
    Msg(String),
}

trait MsgReceiver: Send + Sync {
    fn msg_from_rust(&self, msg: MsgBackend);
}

struct P2PBridge {
    rs_tx: mpsc::UnboundedSender<MsgFrontend>,
    receiver: Arc<dyn MsgReceiver>,
}

impl P2PBridge {
    fn new(receiver: Arc<dyn MsgReceiver>) -> Arc<Self> {
        let (tx, mut rx) = mpsc::unbounded::<MsgFrontend>();
        let bridge = Arc::new(Self {
            rs_tx: tx,
            receiver,
        });

        tokio::spawn(async move {
            while let Ok(msg) = rx.recv().await {
                // TODO: complete this code
            }
        });

        bridge
    }

    fn send_to_backend(&self, msg: MsgFrontend) {
        self.rs_tx.send(msg);
    }

    fn send_to_frontend(&self, msg: MsgBackend) {
        self.receiver.msg_from_rust(msg);
    }
}
