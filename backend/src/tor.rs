use crate::errors::PTPError;
use arti_client::TorClient;
use arti_client::config::TorClientConfig;
use tor_rtcompat::tokio::PreferredRuntime;

//use launch_onion_service_hsid and

async fn generate_onion_address() {}

async fn start_onion_service() -> Result<(), P2PError> {
    let config = TorClientConfig::default();
    let tor_client = TorClient::create_bootstrapped(PreferredRuntime::current()?, config).await?;

    let (service, mut request_stream) = tor_client.onion_service_builder().launch()?;

    println!("Service address: {}", service.onion_name().unwrap());

    while let Some(request) = request_stream.next().await {
        let stream = request.accept().await?;
    }

    Ok(())
}
