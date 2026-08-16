use futures::{SinkExt, StreamExt};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock, mpsc};
use tokio_tungstenite::{WebSocketStream, accept_async, client_async, tungstenite::Message};
use tor_cell::relaycell::msg::Connected;
use tor_rtcompat::PreferredRuntime;

use arti_client::{
    HsId, TorClient, TorClientConfig, config::CfgPath, config::onion_service::OnionServiceConfig,
};
use tor_hsservice::{RunningOnionService, StreamRequest};

use crate::{Result, errors::P2PError, message_queue::FrontendMsg, message_queue::P2PBridge};

// Type aliases for WebSocket read/write halves over Tor stream
type TorDataStream = arti_client::DataStream;
type WsWriter = futures::stream::SplitSink<WebSocketStream<TorDataStream>, Message>;
type WsReader = futures::stream::SplitStream<WebSocketStream<TorDataStream>>;

/// Channel handle used to push messages to an active peer connection loop
pub type PeerTx = mpsc::Sender<Message>;
/// Managed Arti client state
pub struct Client {
    pub tor_client: Arc<TorClient<PreferredRuntime>>,
    pub onion_service: Option<Arc<RunningOnionService>>,
    pub onion_address: Option<HsId>,
    // Persistent active chat connections indexed by target onion address
    pub active_chats: Arc<RwLock<HashMap<String, PeerTx>>>,
}

impl Client {
    /// Until the cache and state dir are not changed the onion address will not changed
    pub async fn new(state_dir: String, cache_dir: String) -> Result<Self> {
        let tor_client = onion_client(state_dir, cache_dir).await?;
        Ok(Self {
            tor_client,
            onion_service: None,
            onion_address: None,
            active_chats: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    pub async fn start_service(
        &mut self,
        nickname: String,
        bridge: Arc<P2PBridge>,
    ) -> Result<HsId> {
        let (service, address) = host_onion_service(
            self.tor_client.clone(),
            nickname,
            self.active_chats.clone(),
            bridge,
        )
        .await?;

        self.onion_service = Some(service);
        self.onion_address = Some(address.clone());
        Ok(address)
    }

    /// Returns the active .onion address if hosted
    // TODO: i guess i have to give Slug or HsId -> Slug -> String
    pub fn get_onion_address_string(&self) -> Option<String> {
        if let Some(ref addr) = self.onion_address {
            // let hsid = format!("{}", addr);
            let hsid = addr.to_string();
            return Some(hsid);
        }
        None
    }

    /// Send a message to an existing chat or connect if not present
    pub async fn send_message(
        &self,
        target_address: &str,
        port: u16,
        msg: FrontendMsg,
    ) -> Result<()> {
        let chats = self.active_chats.write().await;

        if let Some(tx) = chats.get(target_address) {
            tx.send(msg.into())
                .await
                .map_err(|_| P2PError::TorConnectioError)?;
            Ok(())
        } else {
            Err(P2PError::TorConnectioError)
        }
    }
}

/// Initializes and bootstraps the Arti Tor client runtime
pub async fn onion_client(
    state_dir: String,
    cache_dir: String,
) -> Result<Arc<TorClient<PreferredRuntime>>> {
    let mut config_builder = TorClientConfig::builder();

    config_builder.storage().state_dir(CfgPath::new(state_dir));
    config_builder.storage().cache_dir(CfgPath::new(cache_dir));

    let config = config_builder
        .build()
        .map_err(|_| P2PError::TorConnectioError)?;

    let client = TorClient::create_bootstrapped(config)
        .await
        .map_err(|_| P2PError::TorConnectioError)?;

    Ok(client)
}

/// Hosts an Onion Service and spawns a background listener loop to accept multiple incoming streams
// TODO: Research why i need a nick name
pub async fn host_onion_service(
    client: Arc<TorClient<PreferredRuntime>>,
    nickname: String,
    active_chats: Arc<RwLock<HashMap<String, PeerTx>>>,
    bridge: Arc<P2PBridge>,
) -> Result<(Arc<RunningOnionService>, HsId)> {
    let service_config = OnionServiceConfig::builder()
        .nickname(
            nickname
                .parse()
                .map_err(|_| P2PError::OnionConnectionError)?,
        )
        .build()
        .map_err(|_| P2PError::OnionConnectionError)?;

    // Launch service and retrieve incoming requests
    let (service, stream_requests) = match client
        .launch_onion_service(service_config)
        .map_err(|_| P2PError::OnionConnectionError)?
    {
        Some(result) => result,
        // TODO: pass the error to frontend
        None => todo!(),
    };
    let onion_address = service
        .onion_address()
        .ok_or(P2PError::OnionConnectionError)?;

    let stream_handle = tor_hsservice::handle_rend_requests(stream_requests);

    // Spawn async background loop to accept incoming peer connections
    tokio::spawn(handle_incoming_connections(
        stream_handle,
        active_chats,
        bridge,
    ));

    Ok((service, onion_address))
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
            // TODO: What is connected
            let stream = match rend_request.accept(Connected::new_empty()).await {
                Ok(s) => s,
                Err(_) => return,
            };

            if let Ok(ws_stream) = accept_async(stream).await {
                let (mut ws_writer, mut ws_reader) = ws_stream.split();
                let (tx, mut rx) = mpsc::channel::<Message>(100);

                // WARN: Placeholder identity until peer registers address during handshake -> will this
                // work
                let peer_id = format!("peer_{}", rand::random::<u32>());

                {
                    active_chats.write().await.insert(peer_id.clone(), tx);
                }

                // Writer Loop: Flushes outbound messages to network
                tokio::spawn(async move {
                    while let Some(msg) = rx.recv().await {
                        if ws_writer.send(msg).await.is_err() {
                            break;
                        }
                    }
                });

                // Reader Loop: Reads incoming frames and pushes to App Daemon
                while let Some(Ok(msg)) = ws_reader.next().await {
                    if matches!(msg, Message::Close(_)) {
                        break;
                    }
                    bridge_c.send_to_frontend(msg.into());
                }

                // Cleanup on connection drop
                active_chats.write().await.remove(&peer_id);
            }
        });
    }
}

/// Helper function to retrieve an address directly from a RunningOnionService instance
///
// TODO: HsId -> String
pub fn get_onion_address(service: &RunningOnionService) -> Option<String> {
    service.onion_address().map(|name| name.to_string())
}

/// Connects to a remote .onion address and returns split reader/writer channels
pub async fn connect_to_onion_address(
    client: Arc<TorClient<PreferredRuntime>>,
    address: String,
    port: u16,
    active_chats: Arc<Mutex<HashMap<String, PeerTx>>>,
    bridge: P2PBridge,
) -> Result<PeerTx> {
    let target = format!("{}:{}", address, port);

    // Establish raw Tor stream
    let stream = client
        .connect(target.clone())
        .await
        .map_err(|_| P2PError::TorConnectioError)?;

    // Upgrade Tor DataStream to WebSocket client using ws:// protocol
    let ws_url = format!("ws://{}", target);
    // WARN: i guess here the hand shake takes place -> learn the working of tungstenite
    let (ws_stream, _) = client_async(ws_url, stream)
        .await
        .map_err(|_| P2PError::TorConnectioError)?;

    let (mut ws_writer, mut ws_reader) = ws_stream.split();
    let (tx, mut rx) = mpsc::channel::<Message>(100);

    // Register active channel handle
    active_chats
        .lock()
        .await
        .insert(address.clone(), tx.clone());

    // i have a sender in the HashMap so i can write there and this msg will be written the ws stream
    // Outbound Task
    tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if ws_writer.send(msg).await.is_err() {
                break;
            }
        }
    });

    // Inbound Task
    let peer_addr = address.clone();
    tokio::spawn(async move {
        while let Some(Ok(msg)) = ws_reader.next().await {
            if matches!(msg, Message::Close(_)) {
                break;
            }
            bridge.send_to_frontend(msg.into());
        }
        active_chats.lock().await.remove(&peer_addr);
    });

    Ok(tx)
}
