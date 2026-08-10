use futures::StreamExt;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_tungstenite::{accept_async, client_async};
use tor_rtcompat::tokio::TokioNativeTlsRuntime;

use arti_client::{
    TorClient, TorClientConfig, config::CfgPath, config::onion_service::OnionServiceConfig,
};
use tor_hsservice::RunningOnionService;

use crate::{Result, errors::P2PError};

/// Managed Arti client state
pub struct Client {
    pub tor_client: Arc<TorClient<TokioNativeTlsRuntime>>,
    pub onion_service: Option<Arc<RunningOnionService>>,
    pub onion_address: Option<String>,
}

impl Client {
    /// Constructs and initializes a new Arti P2P client instance
    pub async fn new(state_dir: String, cache_dir: String) -> Result<Self> {
        let tor_client = onion_client(state_dir, cache_dir).await?;
        Ok(Self {
            tor_client,
            onion_service: None,
            onion_address: None,
        })
    }

    /// Hosts a hidden service on this client and starts listening for connections
    pub async fn start_service(&mut self, nickname: String) -> Result<String> {
        let (service, address) = host_onion_service(self.tor_client.clone(), nickname).await?;
        self.onion_service = Some(service);
        self.onion_address = Some(address.clone());
        Ok(address)
    }

    /// Returns the active .onion address if hosted
    pub fn get_onion_address(&self) -> Option<String> {
        self.onion_address.clone()
    }
}

/// Initializes and bootstraps the Arti Tor client runtime
pub async fn onion_client(
    state_dir: String,
    cache_dir: String,
) -> Result<Arc<TorClient<TokioNativeTlsRuntime>>> {
    let mut config_builder = TorClientConfig::builder();

    config_builder.storage().state_dir(CfgPath::new(state_dir));
    config_builder.storage().cache_dir(CfgPath::new(cache_dir));

    let config = config_builder
        .build()
        .map_err(|_| P2PError::TorConnectioError)?;

    let runtime = TokioNativeTlsRuntime::current().map_err(|_| P2PError::TorConnectioError)?;

    // Pass runtime and config to bootstrap
    let client = TorClient::create_bootstrapped(runtime, config)
        .await
        .map_err(|_| P2PError::TorConnectioError)?;

    Ok(Arc::new(client))
}

/// Connects to a remote .onion address via standard TCP or upgrades to a WebSocket stream
pub async fn connect_to_onion_address(
    client: Arc<TorClient<TokioNativeTlsRuntime>>,
    address: String,
    port: u16,
    use_websocket: bool,
) -> Result<()> {
    let target = format!("{}:{}", address, port);

    // Establish raw Tor stream
    let stream = client
        .connect(target.clone())
        .await
        .map_err(|_| P2PError::TorConnectioError)?;

    if use_websocket {
        // Upgrade the Tor DataStream to a persistent WebSocket client
        let ws_url = format!("ws://{}", target);
        let (mut ws_stream, _) = client_async(ws_url, stream)
            .await
            .map_err(|_| P2PError::TorConnectioError)?;

        // Send a test text frame
        ws_stream
            .send(tokio_tungstenite::tungstenite::Message::Text(
                "Hello over Tor WS".into(),
            ))
            .await
            .map_err(|_| P2PError::TorConnectioError)?;
    } else {
        // Standard raw TCP stream over Tor
        let (mut reader, mut writer) = tokio::io::split(stream);

        writer
            .write_all(b"PING\n")
            .await
            .map_err(|_| P2PError::TorConnectioError)?;

        let mut response = [0u8; 1024];
        let _bytes_read = reader
            .read(&mut response)
            .await
            .map_err(|_| P2PError::TorConnectioError)?;
    }

    Ok(())
}

/// Hosts an Onion Service and spawns a background listener loop to accept multiple incoming streams
pub async fn host_onion_service(
    client: Arc<TorClient<TokioNativeTlsRuntime>>,
    nickname: String,
) -> Result<(Arc<RunningOnionService>, String)> {
    let service_config = OnionServiceConfig::builder()
        .nickname(
            nickname
                .parse()
                .map_err(|_| P2PError::OnionConnectioError)?,
        )
        .build()
        .map_err(|_| P2PError::OnionConnectioError)?;

    // Launch service and retrieve the incoming stream handle
    let (service, stream_requests) = client
        .launch_onion_service(service_config)
        .await
        .map_err(|_| P2PError::OnionConnectioError)?;

    let onion_address = service
        .onion_name()
        .ok_or(P2PError::OnionConnectioError)?
        .to_string();

    // Convert rendition stream requests into incoming streams
    let stream_handle = tor_hsservice::handle_onion_service_stream(stream_requests);

    // Spawn an async background loop to accept multiple connections
    tokio::spawn(handle_incoming_connections(stream_handle));

    Ok((service, onion_address))
}

/// Connection loop accepting incoming client streams and upgrading to WebSockets or handling raw streams
async fn handle_incoming_connections(
    mut stream_handle: impl futures::Stream<Item = tor_hsservice::RendRequest> + Unpin + Send + 'static,
) {
    while let Some(rend_request) = stream_handle.next().await {
        tokio::spawn(async move {
            // Accept the incoming rendezvous request to obtain an anonymized DataStream
            let stream = match rend_request.accept().await {
                Ok(s) => s,
                Err(_) => return,
            };

            // Upgrade incoming connection to WebSocket
            if let Ok(mut ws_stream) = accept_async(stream).await {
                while let Some(msg) = ws_stream.next().await {
                    match msg {
                        Ok(tokio_tungstenite::tungstenite::Message::Text(text)) => {
                            // Process text frame or echo back
                            let _ = ws_stream
                                .send(tokio_tungstenite::tungstenite::Message::Text(format!(
                                    "Echo: {}",
                                    text
                                )))
                                .await;
                        }
                        Ok(tokio_tungstenite::tungstenite::Message::Close(_)) | Err(_) => break,
                        _ => {}
                    }
                }
            }
        });
    }
}

/// Helper function to retrieve an address directly from a RunningOnionService instance
pub fn get_onion_address(service: &RunningOnionService) -> Option<String> {
    service.onion_name().map(|name| name.to_string())
}
