use std::collections::VecDeque;

use crate::models::Message;

struct IncommingMsgQueue {
    queue: VecDeque<Message>,
}

// TODO:see wht errors can happen here, and handle them
impl IncommingMsgQueue {
    pub fn dequeue(&mut self) -> Message {
        self.queue.pop_front();
    }
}
