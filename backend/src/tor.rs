use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tor_rtcompat::tokio::TokioNativeTlsRuntime;

use arti_client::config::onion_service::OnionServiceConfig;
use arti_client::{TorClient, TorClientConfig, config::CfgPath};
use tor_hsservice::RunningOnionService;

use crate::{Result, errors::P2PError};

/// Initializes and bootstraps an Arti Tor client.
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

    let client = TorClient::create_bootstrapped(config)
        .await
        .map_err(|_| P2PError::TorConnectioError)?;

    Ok(Arc::new(client))
}

pub async fn connect_to_onion_address(
    client: Arc<TorClient<TokioNativeTlsRuntime>>,
    address: String,
    port: u16,
) -> Result<()> {
    let target = format!("{}:{}", address, port);

    let mut stream = client
        .connect(target)
        .await
        .map_err(|_| P2PError::TorConnectioError)?;

    // TODO: connect using ws, or persist the connection
    stream
        .write_all(b"GET / HTTP/1.0\r\nHost: target\r\n\r\n")
        .await
        .map_err(|_| P2PError::TorConnectioError)?;

    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .await
        .map_err(|_| P2PError::TorConnectioError)?;

    Ok(())
}

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

    // TODO: How to accept multiple connections

    let (service, _rend_requests) = client
        .launch_onion_service(service_config)
        .await
        .map_err(|_| P2PError::OnionConnectioError)?;

    let onion_address = service
        .onion_name()
        .ok_or(P2PError::OnionConnectioError)?
        .to_string();

    Ok((service, onion_address))
}

pub async fn get_onion_address() {}
