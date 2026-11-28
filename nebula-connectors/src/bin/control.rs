use std::{net::SocketAddr, process::ExitCode};

use nebula_connector_sdk::validation::decode_control;
use nebula_connectors::control::{Credentials, Runtime};
use tokio::io::AsyncReadExt;
use zeroize::Zeroizing;

const CONTROL_FAILED: &str = "connector_control_failed";

#[tokio::main]
async fn main() -> ExitCode {
    std::panic::set_hook(Box::new(|_| eprintln!("connector_control_panic")));
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(code) => {
            eprintln!("{code}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<(), &'static str> {
    let (address, runtime) = configuration().await.map_err(|_| CONTROL_FAILED)?;
    let listener = tokio::net::TcpListener::bind(address)
        .await
        .map_err(|_| "connector_control_bind_failed")?;
    if address.port() == 0 {
        let bound = listener.local_addr().map_err(|_| CONTROL_FAILED)?;
        println!("connector_control_listening {bound}");
    }
    nebula_connectors::control::serve(listener, runtime, shutdown())
        .await
        .map_err(|_| CONTROL_FAILED)
}

async fn configuration() -> Result<(SocketAddr, Runtime), Box<dyn std::error::Error>> {
    let address: SocketAddr = std::env::var("CONNECTOR_CONTROL_LISTEN")
        .unwrap_or_else(|_| "127.0.0.1:8092".into())
        .parse()?;
    let file = tokio::fs::File::open(std::env::var("CONNECTOR_CONTROL_CREDENTIALS_FILE")?).await?;
    let mut bytes = Zeroizing::new(Vec::new());
    file.take(4097).read_to_end(&mut bytes).await?;
    let credentials: Credentials = decode_control(&bytes, 4096)?;
    Ok((address, Runtime::new(credentials)?))
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
