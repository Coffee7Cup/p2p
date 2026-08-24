use arti_client::{
    TorClient, TorClientConfig,
    config::{CfgPath, onion_service::OnionServiceConfig},
};
use futures::{SinkExt, StreamExt};
use safelog::DisplayRedacted;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::{RwLock, mpsc};
use tokio_tungstenite::{
    accept_async, client_async,
    tungstenite::{Message, Utf8Bytes},
};
use tor_cell::relaycell::msg::Connected;
use tor_hsservice::{RunningOnionService, StreamRequest};
use tor_rtcompat::PreferredRuntime;
use tracing::{error, info, warn};

use crate::Result;
use crate::errors::P2PError;

#[derive(uniffi::Enum)]
pub enum TorStatus {
    Online,
    Offline,
    Connecting,
}

#[derive(uniffi::Enum)]
pub enum BackendMsg {
    ChatMsg { text: String },
    Error { message: String },
    TorStatus { status: TorStatus },
}

impl From<Message> for BackendMsg {
    fn from(msg: Message) -> Self {
        match msg {
            Message::Text(text) => Self::ChatMsg {
                text: text.to_string(),
            },
            _ => Self::Error {
                message: "Cannot read the message".to_string(),
            },
        }
    }
}

#[uniffi::export(with_foreign)]
pub trait MsgReceiver: Send + Sync {
    fn msg_from_rust(&self, msg: BackendMsg);
}

type PeerTx = mpsc::Sender<Message>;

#[derive(uniffi::Object)]
pub struct Client {
    tor_client: Arc<TorClient<PreferredRuntime>>,
    onion_service: Arc<RwLock<Option<Arc<RunningOnionService>>>>,
    onion_address: Arc<RwLock<Option<String>>>,
    active_chats: Arc<RwLock<HashMap<String, PeerTx>>>,
    receiver: Arc<dyn MsgReceiver>,
}

#[uniffi::export(async_runtime = "tokio")]
pub async fn create_client(
    state_dir: String,
    cache_dir: String,
    receiver: Arc<dyn MsgReceiver>,
) -> Result<Arc<Client>> {
        info!("Initializing Tor client...");
        receiver.msg_from_rust(BackendMsg::TorStatus {
            status: TorStatus::Connecting,
        });

        let mut config_builder = TorClientConfig::builder();
        config_builder.storage().state_dir(CfgPath::new(state_dir));
        config_builder.storage().cache_dir(CfgPath::new(cache_dir));

        let config = config_builder
            .build()
            .map_err(|e| P2PError::TorConnectionError(e.to_string()))?;

        let tor_client = TorClient::create_bootstrapped(config).await.map_err(|e| {
            error!("Failed to bootstrap Tor client: {}", e);
            receiver.msg_from_rust(BackendMsg::TorStatus {
                status: TorStatus::Offline,
            });
            P2PError::TorConnectionError(e.to_string())
        })?;

        receiver.msg_from_rust(BackendMsg::TorStatus {
            status: TorStatus::Online,
        });

        Ok(Arc::new(Client {
            tor_client,
            onion_service: Arc::new(RwLock::new(None)),
            onion_address: Arc::new(RwLock::new(None)),
            active_chats: Arc::new(RwLock::new(HashMap::new())),
            receiver,
        }))
    }

#[uniffi::export(async_runtime = "tokio")]
impl Client {
    pub async fn start_service(self: Arc<Self>, nickname: String) -> Result<String> {
        info!("Starting Onion Service with nickname: {}", nickname);

        let service_config = OnionServiceConfig::builder()
            .nickname(
                nickname
                    .parse()
                    .map_err(|_| P2PError::OnionConnectionError("Invalid nickname".to_string()))?,
            )
            .build()
            .map_err(|e| {
                error!("Failed to build Onion Service config: {}", e);
                P2PError::OnionConnectionError(e.to_string())
            })?;

        let (service, stream_requests) = self
            .tor_client
            .launch_onion_service(service_config)
            .map_err(|e| {
                error!("Failed to launch Onion Service: {}", e);
                P2PError::OnionConnectionError(e.to_string())
            })?
            .ok_or_else(|| {
                error!("Onion Service launch returned None");
                P2PError::OnionConnectionError("Service returned None".to_string())
            })?;

        let address = service.onion_address().ok_or_else(|| {
            error!("Failed to get Onion Address");
            P2PError::OnionConnectionError("Failed to get address".to_string())
        })?;

        let address_str = address.display_unredacted().to_string();
        info!("Service started successfully. Address: {}", address_str);

        *self.onion_service.write().await = Some(service);
        *self.onion_address.write().await = Some(address_str.clone());

        let stream_handle = tor_hsservice::handle_rend_requests(stream_requests);
        let receiver = self.receiver.clone();
        let active_chats = self.active_chats.clone();

        tokio::spawn(Self::handle_incoming_connections(
            stream_handle,
            active_chats,
            receiver,
        ));

        Ok(address_str)
    }

    pub async fn get_onion_address_string(&self) -> Option<String> {
        self.onion_address.read().await.clone()
    }

    pub async fn send_message(&self, target_address: String, msg: String) -> Result<()> {
        info!("Sending message to {}", target_address);
        let chats = self.active_chats.read().await;

        if let Some(tx) = chats.get(&target_address) {
            tx.send(Message::Text(Utf8Bytes::from(msg)))
                .await
                .map_err(|e| {
                    error!("Failed to send message over channel: {}", e);
                    P2PError::MessageSendError(e.to_string())
                })?;
            Ok(())
        } else {
            error!("Chat not found for address: {}", target_address);
            Err(P2PError::ChatNotFoundError(target_address))
        }
    }

    pub async fn connect_to_onion_address(&self, address: String, port: u16) -> Result<()> {
        info!("Connecting to {}:{}", address, port);
        let target = format!("{}:{}", address, port);

        let stream = self.tor_client.connect(target.clone()).await.map_err(|e| {
            error!("Failed to connect to stream: {}", e);
            P2PError::TorConnectionError(e.to_string())
        })?;

        let ws_url = format!("ws://{}", target);
        let (ws_stream, _) = client_async(ws_url, stream).await.map_err(|e| {
            error!("Failed to upgrade to WebSocket: {}", e);
            P2PError::TorConnectionError(e.to_string())
        })?;

        let (mut ws_writer, mut ws_reader) = ws_stream.split();
        let (tx, mut rx) = mpsc::channel::<Message>(100);

        self.active_chats
            .write()
            .await
            .insert(address.clone(), tx.clone());

        let local_onion_addr = self.get_onion_address_string().await.unwrap_or_default();

        // Outbound Task
        tokio::spawn(async move {
            // This the first msg to be sent on ws and other msgs starting wiht ### for_app will be
            // parsed and used by app

            // TODO: Have Some standard coded in something like constants.rs file so i dont have to
            // remember ### for_app etc
            let metadata = format!("### for_app: {}", local_onion_addr);
            if let Err(e) = ws_writer
                .send(Message::Text(Utf8Bytes::from(metadata)))
                .await
            {
                warn!("Failed to send metadata: {}", e);
            }

            while let Some(msg) = rx.recv().await {
                if ws_writer.send(msg).await.is_err() {
                    break;
                }
            }
        });

        // Inbound Task
        let active_chats = self.active_chats.clone();
        let peer_addr = address.clone();
        let receiver = self.receiver.clone();

        tokio::spawn(async move {
            while let Some(Ok(msg)) = ws_reader.next().await {
                if matches!(msg, Message::Close(_)) {
                    break;
                }
                receiver.msg_from_rust(msg.into());
            }
            active_chats.write().await.remove(&peer_addr);
            info!("Connection with {} closed", peer_addr);
        });

        Ok(())
    }
}

impl Client {
    async fn handle_incoming_connections(
        mut stream_handle: impl futures::Stream<Item = StreamRequest> + Unpin + Send + 'static,
        active_chats: Arc<RwLock<HashMap<String, PeerTx>>>,
        receiver: Arc<dyn MsgReceiver>,
    ) {
        while let Some(rend_request) = stream_handle.next().await {
            let active_chats = active_chats.clone();
            let receiver_c = receiver.clone();

            tokio::spawn(async move {
                let stream = match rend_request.accept(Connected::new_empty()).await {
                    Ok(s) => s,
                    Err(e) => {
                        warn!("Failed to accept rendezvous request: {}", e);
                        return;
                    }
                };

                if let Ok(ws_stream) = accept_async(stream).await {
                    let (mut ws_writer, mut ws_reader) = ws_stream.split();
                    let (tx, mut rx) = mpsc::channel::<Message>(100);

                    let mut peer_id = format!(
                        "peer_{}",
                        std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_micros()
                    );

                    // Writer Loop
                    tokio::spawn(async move {
                        while let Some(msg) = rx.recv().await {
                            if ws_writer.send(msg).await.is_err() {
                                break;
                            }
                        }
                    });

                    // Reader Loop
                    while let Some(Ok(msg)) = ws_reader.next().await {
                        match msg {
                            Message::Text(text) => {
                                let text_str = text.as_str();
                                if let Some(addr) = text_str.strip_prefix("### for_app: ") {
                                    peer_id = addr.trim().to_string();
                                    info!("Identified peer connection: {}", peer_id);
                                    active_chats
                                        .write()
                                        .await
                                        .insert(peer_id.clone(), tx.clone());
                                } else {
                                    receiver_c.msg_from_rust(Message::Text(text).into());
                                }
                            }
                            _ => break,
                        }
                    }

                    active_chats.write().await.remove(&peer_id);
                    info!("Incoming connection from {} closed", peer_id);
                }
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_tungstenite::tungstenite::protocol::Message;

    #[test]
    fn test_backend_msg_from_text() {
        let text_msg = Message::Text(Utf8Bytes::from("Hello, World!"));
        let backend_msg: BackendMsg = text_msg.into();

        match backend_msg {
            BackendMsg::ChatMsg { text } => {
                assert_eq!(text, "Hello, World!");
            }
            _ => panic!("Expected ChatMsg"),
        }
    }

    #[test]
    fn test_backend_msg_from_other() {
        let binary_msg = Message::Binary(vec![1, 2, 3].into());
        let backend_msg: BackendMsg = binary_msg.into();

        match backend_msg {
            BackendMsg::Error { message } => {
                assert_eq!(message, "Cannot read the message");
            }
            _ => panic!("Expected Error"),
        }
    }
}

