// TODO: modify these functions according to message_queue.rs

use futures::{SinkExt, StreamExt};
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::{Mutex, mpsc},
};
use tokio_tungstenite::{WebSocketStream, accept_async, client_async, tungstenite::Message};
use tor_rtcompat::tokio::TokioNativeTlsRuntime;

use arti_client::{
    TorClient, TorClientConfig, config::CfgPath, config::onion_service::OnionServiceConfig,
};
use tor_hsservice::RunningOnionService;

use crate::{Result, errors::P2PError};

// Type aliases for WebSocket read/write halves over Tor stream
type TorDataStream = arti_client::DataStream;
type WsWriter = futures::stream::SplitSink<WebSocketStream<TorDataStream>, Message>;
type WsReader = futures::stream::SplitStream<WebSocketStream<TorDataStream>>;

/// Channel handle used to push messages to an active peer connection loop
pub type PeerTx = mpsc::Sender<Message>;

// WARN: The msg_tx_to_app is type mpsc::Sender<String,Message> i feel like i should have an enum instad of Message to that the frontend
// act accordingly, and mpsc::Sender? man will it work

/// Managed Arti client state
pub struct Client {
    pub tor_client: Arc<TorClient<TokioNativeTlsRuntime>>,
    pub onion_service: Option<Arc<RunningOnionService>>,
    pub onion_address: Option<String>,
    // Persistent active chat connections indexed by target onion address
    pub active_chats: Arc<RwLock<HashMap<String, PeerTx>>>,
}

impl Client {
    /// Until the cache and state dir are not changed the onion address will not changed
    pub async fn new(state_dir: String, cache_dir: String) -> Result<Self> {
        let tor_client = onion_client(&state_dir, &cache_dir).await?;
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
        msg_tx_to_app: mpsc::Sender<(String, Message)>,
        // TODO: Decide if tthe mpcs is okay or should i switch -> something the uniffi provides
    ) -> Result<String> {
        let (service, address) = host_onion_service(
            self.tor_client.clone(),
            nickname,
            self.active_chats.clone(),
            msg_tx_to_app,
        )
        .await?;

        self.onion_service = Some(service);
        self.onion_address = Some(address.clone());
        Ok(address)
    }

    /// Returns the active .onion address if hosted
    pub fn get_onion_address(&self) -> Option<String> {
        self.onion_address.clone()
    }

    /// Send a message to an existing chat or connect if not present
    // TODO: i guess i will expose a callback, what will flush all the messages - may be this is not
    // good
    // TODO: im passing Message type? shouldnt i send String and then convert it to Message or even
    // better a ENUM and act accordingly
    pub async fn send_message(&self, target_address: &str, port: u16, msg: Message) -> Result<()> {
        let mut chats = self.active_chats.lock().await;

        if let Some(tx) = chats.get(target_address) {
            tx.send(msg)
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
    state_dir: &str,
    cache_dir: &str,
) -> Result<Arc<TorClient<TokioNativeTlsRuntime>>> {
    let mut config_builder = TorClientConfig::builder();

    config_builder
        .storage()
        .state_dir(CfgPath::new_literal(state_dir));
    config_builder
        .storage()
        .cache_dir(CfgPath::new_literal(cache_dir));

    let config = config_builder
        .build()
        .map_err(|_| P2PError::TorConnectioError)?;

    let client = TorClient::create_bootstrapped(config)
        .await
        .map_err(|_| P2PError::TorConnectioError)?;

    Ok(Arc::new(client))
}

/// Hosts an Onion Service and spawns a background listener loop to accept multiple incoming streams
pub async fn host_onion_service(
    client: Arc<TorClient<TokioNativeTlsRuntime>>,
    nickname: String,
    active_chats: Arc<RwLock<HashMap<String, PeerTx>>>,
    msg_tx_to_app: mpsc::Sender<(String, Message)>,
) -> Result<(Arc<RunningOnionService>, String)> {
    let service_config = OnionServiceConfig::builder()
        .nickname(
            nickname
                .parse()
                .map_err(|_| P2PError::OnionConnectioError)?,
        )
        .build()
        .map_err(|_| P2PError::OnionConnectioError)?;

    // Launch service and retrieve incoming requests
    let (service, stream_requests) = client
        .launch_onion_service(service_config)
        .await
        .map_err(|_| P2PError::OnionConnectioError)?;

    let onion_address = service
        .onion_name()
        .ok_or(P2PError::OnionConnectioError)?
        .to_string();

    let stream_handle = tor_hsservice::handle_rend_requests(stream_requests);

    // Spawn async background loop to accept incoming peer connections
    tokio::spawn(handle_incoming_connections(
        stream_handle,
        active_chats,
        msg_tx_to_app,
    ));

    Ok((Arc::new(service), onion_address))
}

/// Connection loop accepting incoming client streams and upgrading to WebSockets
///
// WARN: this will run until the stream is active
async fn handle_incoming_connections(
    mut stream_handle: impl futures::Stream<Item = tor_hsservice::RendRequest> + Unpin + Send + 'static,
    active_chats: Arc<RwLock<HashMap<String, PeerTx>>>,
    msg_tx_to_app: mpsc::Sender<(String, Message)>,
) {
    while let Some(rend_request) = stream_handle.next().await {
        let active_chats = active_chats.clone();
        let msg_tx_to_app = msg_tx_to_app.clone();

        tokio::spawn(async move {
            let stream = match rend_request.accept().await {
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
                    active_chats.lock().await.insert(peer_id.clone(), tx);
                }

                // Writer Loop: Flushes outbound messages to network
                tokio::spawn(async move {
                    while let Some(msg) = rx.recv().await {
                        if ws_writer.send(msg).await.is_err() {
                            break;
                        }
                    }
                });

                // WARN: i dont know if the uniffi provides the mspc::Sender or equivalent

                // Reader Loop: Reads incoming frames and pushes to App Daemon
                while let Some(Ok(msg)) = ws_reader.next().await {
                    if matches!(msg, Message::Close(_)) {
                        break;
                    }
                    let _ = msg_tx_to_app.send((peer_id.clone(), msg)).await;
                }

                // Cleanup on connection drop
                active_chats.lock().await.remove(&peer_id);
            }
        });
    }
}

/// Helper function to retrieve an address directly from a RunningOnionService instance
pub fn get_onion_address(service: &RunningOnionService) -> Option<String> {
    service.onion_address().map(|name| name.to_string())
}

/// Connects to a remote .onion address and returns split reader/writer channels
pub async fn connect_to_onion_address(
    client: Arc<TorClient<TokioNativeTlsRuntime>>,
    address: String,
    port: u16,
    active_chats: Arc<Mutex<HashMap<String, PeerTx>>>,
    msg_tx_to_app: mpsc::Sender<(String, Message)>,
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
            let _ = msg_tx_to_app.send((peer_addr.clone(), msg)).await;
        }
        active_chats.lock().await.remove(&peer_addr);
    });

    Ok(tx)
}
