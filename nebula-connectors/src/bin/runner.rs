use std::process::ExitCode;

use nebula_connector_sdk::{Client, Connector, runner, validation::decode_control};
use nebula_connectors::slack::Slack;
use serde::Deserialize;
use serde_json::json;
use tokio::io::AsyncReadExt;
use zeroize::{Zeroize, Zeroizing};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Configuration {
    endpoint: String,
    workspace_id: String,
    collection_id: String,
    connection_id: String,
    run_id: String,
    scan_id: String,
    complete_inventory: bool,
    bearer: String,
}

impl Drop for Configuration {
    fn drop(&mut self) {
        self.bearer.zeroize();
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    std::panic::set_hook(Box::new(|_| eprintln!("connector_runner_panic")));
    let result = tokio::select! {
        result = execute() => result,
        _ = shutdown() => Err("connector_runner_cancelled".into()),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(code) => {
            eprintln!("{code}");
            ExitCode::FAILURE
        }
    }
}

async fn execute() -> Result<(), String> {
    let mut args = std::env::args().skip(1);
    let provider = args.next();
    let command = args.next();
    if provider.as_deref() != Some("slack") || args.next().is_some() {
        return Err("usage: nebula-connector slack <spec|run>".into());
    }
    match command.as_deref() {
        Some("spec") => {
            println!(
                "{}",
                serde_json::to_string(&Slack.specification())
                    .map_err(|_| "connector_specification_failed")?
            );
            Ok(())
        }
        Some("run") => run().await,
        _ => Err("usage: nebula-connector slack <spec|run>".into()),
    }
}

async fn run() -> Result<(), String> {
    let path =
        std::env::var("CONNECTOR_RUN_FILE").map_err(|_| "connector_run_configuration_invalid")?;
    let file = tokio::fs::File::open(path)
        .await
        .map_err(|_| "connector_run_configuration_invalid")?;
    let mut bytes = Zeroizing::new(Vec::new());
    file.take(65537)
        .read_to_end(&mut bytes)
        .await
        .map_err(|_| "connector_run_configuration_invalid")?;
    let mut config: Configuration =
        decode_control(&bytes, 65536).map_err(|_| "connector_run_configuration_invalid")?;
    bytes.zeroize();
    let client = Client::new(
        &config.endpoint,
        &config.workspace_id,
        &config.collection_id,
        &config.connection_id,
        &config.bearer,
    )
    .map_err(|error| error.to_string())?;
    config.bearer.zeroize();
    let completed = runner::run(
        &Slack,
        client,
        &config.run_id,
        &config.scan_id,
        config.complete_inventory,
    )
    .await
    .map_err(|error| error.to_string())?;
    println!(
        "{}",
        json!({"run_id":completed.id,"scan_id":completed.scan_id,"state":completed.state})
    );
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
