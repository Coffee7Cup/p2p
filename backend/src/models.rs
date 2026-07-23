use crate::errors::PTPError;

pub enum Message {
    Error(PTPError),
}
