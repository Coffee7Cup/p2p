use tokio::sync::mpsc::{self, Receiver, Sender};

use crate::{errors::PTPError, models::FrontendMsg};

struct WorkQueue {
    sender: Sender<FrontendMsg>,
    receiver: Receiver<FrontendMsg>,
}

impl WorkQueue {
    pub fn new(limit: usize) -> Self {
        let (s, r) = mpsc::channel(limit);
        Self {
            sender: s,
            receiver: r,
        }
    }

    pub fn producer(&self) -> Sender<FrontendMsg> {
        self.sender.clone()
    }

    pub fn enqueue(&self, msg: FrontendMsg) -> Result<(), PTPError> {
        self.sender
            .try_send(msg)
            .map_err(|_| PTPError::QueueBufferOverflowError)
    }

    pub async fn process_works(&mut self) {
        while let Some(msg) = self.receiver.recv().await {
            match msg {}
        }
    }
}
