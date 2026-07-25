use std::sync::mpsc::Sender;

use chrono::{DateTime, Utc};

use crate::errors::PTPError;

pub enum FrontendMsg {}

pub enum TorConnectionStatus {
    Active,
    Connecting,
    Disconnected,
}

pub enum BackendMsg {
    Error(PTPError),
    TorConnectionStatus,
}

pub enum ErrorMsg {}

// pub struct ChatMsg<'a> {
//     id: ,
//     Sender: ,
//     time: DateTime<Utc>,
//     contant: &'a str,
// }
//
// pub struct Chat {}
//
// impl ChatMsg {
//     pub fn  create_id (){
//         // TODO: Here, the hash sender dateTime etc to create a simple and fast hash
//     }
// }
