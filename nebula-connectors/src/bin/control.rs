use std::{net::SocketAddr, process::ExitCode};

use nebula_connector_sdk::validation::decode_control;
use nebula_connectors::control::{Credentials, Runtime};
use tokio::io::AsyncReadExt;
use zeroize::Zeroizing;

#[tokio::main]
async fn main() -> ExitCode {
    std::panic::set_hook(Box::new(|_| eprintln!("connector_control_panic")));
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(_) => {
            eprintln!("connector_control_failed");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let address: SocketAddr = std::env::var("CONNECTOR_CONTROL_LISTEN")
        .unwrap_or_else(|_| "127.0.0.1:8092".into())
        .parse()?;
    let file = tokio::fs::File::open(std::env::var("CONNECTOR_CONTROL_CREDENTIALS_FILE")?).await?;
    let mut bytes = Zeroizing::new(Vec::new());
    file.take(4097).read_to_end(&mut bytes).await?;
    let credentials: Credentials = decode_control(&bytes, 4096)?;
    let runtime = Runtime::new(credentials)?;
    let listener = tokio::net::TcpListener::bind(address).await?;
    nebula_connectors::control::serve(listener, runtime, shutdown()).await?;
    Ok(())
}

async fn shutdown() {
    #[cfg(unix)]
    {
        if let Ok(mut terminate) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            tokio::select! { _ = terminate.recv() => {}, _ = tokio::signal::ctrl_c() => {} }
            return;
        }
    }
    let _ = tokio::signal::ctrl_c().await;
}
