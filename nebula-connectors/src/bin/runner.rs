#[path = "runner/agent.rs"]
mod agent;

use std::process::ExitCode;

use nebula_connector_sdk::{Client, Connector, runner, validation::decode_control};
use nebula_connectors::{
    box_files::BoxFiles,
    confluence::Confluence,
    dropbox::Dropbox,
    github::GitHub,
    gmail::Gmail,
    google_drive::GoogleDrive,
    jira::Jira,
    microsoft_files::{OneDrive, SharePoint},
    notion::Notion,
    outlook::Outlook,
    slack::Slack,
    teams::Teams,
};
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

const USAGE: &str = "usage: nebula-connector <slack|google_drive|gmail|onedrive|sharepoint|outlook|teams|notion|confluence|jira|box|dropbox|github> <spec|run> | pair --endpoint URL [--state-file PATH] | serve [--state-file PATH]";

async fn execute() -> Result<(), String> {
    let mut args = std::env::args().skip(1);
    let provider = args.next();
    if matches!(provider.as_deref(), Some("pair" | "serve")) {
        return agent::execute(provider.as_deref().unwrap(), args.collect()).await;
    }
    let command = args.next();
    if args.next().is_some() {
        return Err(USAGE.into());
    }
    let connector = connector(provider.as_deref().unwrap_or(""))?;
    match command.as_deref() {
        Some("spec") => {
            println!(
                "{}",
                serde_json::to_string(&connector.specification())
                    .map_err(|_| "connector_specification_failed")?
            );
            Ok(())
        }
        Some("run") => run(connector).await,
        _ => Err(USAGE.into()),
    }
}

async fn run(connector: &dyn Connector) -> Result<(), String> {
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
        connector,
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

fn connector(provider: &str) -> Result<&'static dyn Connector, String> {
    Ok(match provider {
        "slack" => &Slack,
        "google_drive" => &GoogleDrive,
        "gmail" => &Gmail,
        "onedrive" => &OneDrive,
        "sharepoint" => &SharePoint,
        "outlook" => &Outlook,
        "teams" => &Teams,
        "notion" => &Notion,
        "confluence" => &Confluence,
        "jira" => &Jira,
        "box" => &BoxFiles,
        "dropbox" => &Dropbox,
        "github" => &GitHub,
        _ => return Err(USAGE.into()),
    })
}

const PROVIDERS: &[&str] = &[
    "slack",
    "google_drive",
    "gmail",
    "onedrive",
    "sharepoint",
    "outlook",
    "teams",
    "notion",
    "confluence",
    "jira",
    "box",
    "dropbox",
    "github",
];
