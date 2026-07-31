use tokio::sync::mpsc::{self, Receiver, Sender};

use crate::{
    errors::PTPError,
    models::{BackendMsg, ErrorMsg, FrontendMsg},
};

// TODO: #1 - remember to push the pending tasks to the error to be processed later or to intimatate the frontend
// TODO: #2 - maybe add a interface so that i can match types easyly "process_works"

struct WorkQueue<T> {
    sender: Sender<T>,
    receiver: Receiver<T>,
}

impl<T> WorkQueue<T> {
    pub fn new(limit: usize) -> Self {
        let (s, r) = mpsc::channel(limit);
        Self {
            sender: s,
            receiver: r,
        }
    }

    pub fn sender(&self) -> Sender<T> {
        self.sender.clone()
    }

    pub fn enqueue(&self, msg: T) -> Result<(), PTPError> {
        self.sender
            .try_send(msg)
            .map_err(|_| PTPError::QueueBufferOverflowError)
    }
}

type BackendWorkQueue = WorkQueue<BackendMsg>;
type FrontendWorkQueue = WorkQueue<FrontendMsg>;
type ErrorWorkQueue = WorkQueue<ErrorMsg>;

impl BackendWorkQueue {
    pub async fn process_works(&mut self) {
        while let Some(msg) = self.receiver.recv().await {
            match msg {}
        }
    }
}

impl FrontendWorkQueue {
    pub async fn process_works(&mut self) {
        while let Some(msg) = self.receiver.recv().await {
            match msg {}
        }
    }
}

impl ErrorWorkQueue {
    pub async fn process_works(&mut self) {
        while let Some(msg) = self.receiver.recv().await {
            match msg {}
        }
    }
}
