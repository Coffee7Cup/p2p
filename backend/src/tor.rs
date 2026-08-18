use futures::{SinkExt, StreamExt};
use safelog::DisplayRedacted;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{RwLock, mpsc};
use tokio_tungstenite::{
    WebSocketStream, accept_async, client_async,
    tungstenite::{Message, Utf8Bytes},
};
use tor_cell::relaycell::msg::Connected;
use tor_rtcompat::PreferredRuntime;

use arti_client::{
    HsId, TorClient, TorClientConfig, config::CfgPath, config::onion_service::OnionServiceConfig,
};
use tor_hsservice::{RunningOnionService, StreamRequest};

use crate::{Result, errors::P2PError, message_queue::P2PBridge};

// Type aliases for WebSocket read/write halves over Tor stream
type TorDataStream = arti_client::DataStream;

/// Channel handle used to push messages to an active peer connection loop
pub type PeerTx = mpsc::Sender<Message>;

/// Managed Arti client state
pub struct Client {
    pub tor_client: Arc<TorClient<PreferredRuntime>>,
    pub onion_service: Option<Arc<RunningOnionService>>,
    pub onion_address: Option<HsId>,
    // Persistent active chat connections indexed by target onion address
    pub active_chats: Arc<RwLock<HashMap<String, PeerTx>>>,
    pub bridge: Arc<P2PBridge>,
}

impl Client {
    /// Bootstraps and initializes a new Tor Client instance
    pub async fn new(
        state_dir: String,
        cache_dir: String,
        bridge: Arc<P2PBridge>,
    ) -> Result<Arc<Self>> {
        let mut config_builder = TorClientConfig::builder();
        config_builder.storage().state_dir(CfgPath::new(state_dir));
        config_builder.storage().cache_dir(CfgPath::new(cache_dir));

        let config = config_builder
            .build()
            .map_err(|_| P2PError::TorConnectioError)?;

        let tor_client = TorClient::create_bootstrapped(config)
            .await
            .map_err(|_| P2PError::TorConnectioError)?;

        Ok(Arc::new(Self {
            tor_client,
            onion_service: None,
            onion_address: None,
            bridge,
            active_chats: Arc::new(RwLock::new(HashMap::new())),
        }))
    }

    /// Hosts an Onion Service and spawns background listener loop
    pub async fn start_service(&mut self, nickname: String) -> Result<HsId> {
        let service_config = OnionServiceConfig::builder()
            .nickname(
                nickname
                    .parse()
                    .map_err(|_| P2PError::OnionConnectionError)?,
            )
            .build()
            .map_err(|_| P2PError::OnionConnectionError)?;

        // Launch service and retrieve incoming requests
        let (service, stream_requests) = self
            .tor_client
            .launch_onion_service(service_config)
            .map_err(|_| P2PError::OnionConnectionError)?
            .ok_or(P2PError::TorConnectioError)?;

        let address = service
            .onion_address()
            .ok_or(P2PError::OnionConnectionError)?;

        let stream_handle = tor_hsservice::handle_rend_requests(stream_requests);
        let bridge = self.bridge.clone();
        // Spawn async background loop to accept incoming peer connections
        tokio::spawn(Self::handle_incoming_connections(
            stream_handle,
            self.active_chats.clone(),
            bridge,
        ));

        self.onion_service = Some(service);
        self.onion_address = Some(address.clone());

        Ok(address)
    }

    /// Returns the active .onion address as an unredacted string
    pub fn get_onion_address_string(&self) -> Option<String> {
        self.onion_address
            .as_ref()
            .map(|addr| addr.display_unredacted().to_string())
    }

    /// Send a message to an existing active chat
    pub async fn send_message(&self, target_address: &str, msg: String) -> Result<()> {
        let chats = self.active_chats.read().await;

        if let Some(tx) = chats.get(target_address) {
            tx.send(Message::Text(msg.into()))
                .await
                .map_err(|_| P2PError::TorConnectioError)?;
            Ok(())
        } else {
            Err(P2PError::TorConnectioError)
        }
    }

    /// Connects to a remote .onion address and returns split reader/writer channels
    pub async fn connect_to_onion_address(&self, address: String, port: u16) -> Result<PeerTx> {
        let target = format!("{}:{}", address, port);

        // Establish raw Tor stream
        let stream = self
            .tor_client
            .connect(target.clone())
            .await
            .map_err(|_| P2PError::TorConnectioError)?;

        // Upgrade Tor DataStream to WebSocket client using ws:// protocol
        let ws_url = format!("ws://{}", target);
        let (ws_stream, _) = client_async(ws_url, stream)
            .await
            .map_err(|_| P2PError::TorConnectioError)?;

        let (mut ws_writer, mut ws_reader) = ws_stream.split();
        let (tx, mut rx) = mpsc::channel::<Message>(100);

        // Register active channel handle
        self.active_chats
            .write()
            .await
            .insert(address.clone(), tx.clone());

        // Send identity handshake if hosted
        // TODO: See what to do with unwrap_or_default
        let local_onion_addr = self.get_onion_address_string().unwrap_or_default();

        // Outbound Task
        tokio::spawn(async move {
            let _ = ws_writer
                .send(Message::Text(Utf8Bytes::from(format!(
                    "### for_app: {local_onion_addr}"
                ))))
                .await;

            while let Some(msg) = rx.recv().await {
                if ws_writer.send(msg).await.is_err() {
                    break;
                }
            }
        });

        // Inbound Task
        let active_chats = self.active_chats.clone();
        let peer_addr = address.clone();
        let bridge = self.bridge.clone();
        tokio::spawn(async move {
            while let Some(Ok(msg)) = ws_reader.next().await {
                if matches!(msg, Message::Close(_)) {
                    break;
                }
                bridge.send_to_frontend(msg.into());
            }
            active_chats.write().await.remove(&peer_addr);
        });

        Ok(tx)
    }

    /// Connection loop accepting incoming client streams and upgrading to WebSockets
    async fn handle_incoming_connections(
        mut stream_handle: impl futures::Stream<Item = StreamRequest> + Unpin + Send + 'static,
        active_chats: Arc<RwLock<HashMap<String, PeerTx>>>,
        bridge: Arc<P2PBridge>,
    ) {
        while let Some(rend_request) = stream_handle.next().await {
            let active_chats = active_chats.clone();
            let bridge_c = bridge.clone();

            tokio::spawn(async move {
                let stream = match rend_request.accept(Connected::new_empty()).await {
                    Ok(s) => s,
                    Err(_) => return,
                };

                if let Ok(ws_stream) = accept_async(stream).await {
                    let (mut ws_writer, mut ws_reader) = ws_stream.split();
                    let (tx, mut rx) = mpsc::channel::<Message>(100);

                    let mut peer_id = format!("peer_{}", rand::random::<u32>());

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
                                // TODO: i no need to check this every time, the for_app msgs are
                                // only send at the start
                                let text_str = text.as_str();
                                if let Some(addr) = text_str.strip_prefix("### for_app: ") {
                                    peer_id = addr.trim().to_string();
                                } else {
                                    // Forward to frontend if it wasn't metadata
                                    bridge_c.send_to_frontend(Message::Text(text).into());
                                }
                            }

                            _ => break,
                        }
                    }

                    active_chats.write().await.insert(peer_id.clone(), tx);
                    // Cleanup on connection drop
                    active_chats.write().await.remove(&peer_id);
                }
            });
        }
    }
}
