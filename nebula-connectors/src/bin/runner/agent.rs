#[path = "credentials.rs"]
mod credentials;
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use credentials::{CredentialFile, Credentials, Pairing};
use nebula_connector_sdk::{Client, runner};
use reqwest::{Client as HttpClient, Url, header::HeaderValue};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    path::PathBuf,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{task::JoinSet, time::Instant};
use zeroize::{Zeroize, Zeroizing};

const USAGE: &str = "usage: nebula-connector pair --endpoint URL [--state-file PATH] [--label LABEL] | serve [--state-file PATH]";
const WORK_LIMIT: usize = 4;

pub async fn execute(command: &str, args: Vec<String>) -> Result<(), String> {
    let (mut endpoint, mut state_path, mut label) = (None, None, None);
    let mut args = args.into_iter();
    while let Some(flag) = args.next() {
        let value = args.next().ok_or(USAGE)?;
        match flag.as_str() {
            "--endpoint" if command == "pair" && endpoint.is_none() => endpoint = Some(value),
            "--state-file" if state_path.is_none() => state_path = Some(PathBuf::from(value)),
            "--label" if command == "pair" && label.is_none() => label = Some(value),
            _ => return Err(USAGE.into()),
        }
    }
    let path = state_path
        .or_else(|| {
            std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".nebula-connector/runner.json"))
        })
        .ok_or(USAGE)?;
    let file = CredentialFile::open(path)?;
    if command == "pair" {
        let endpoint = endpoint.ok_or(USAGE)?;
        let transport = Transport::new(&endpoint)?;
        let mut state = match file.read()? {
            Some(state) if state.endpoint != endpoint => {
                return Err("runner_endpoint_mismatch".into());
            }
            Some(state) => state,
            None => Credentials {
                endpoint,
                runner_id: None,
                secret: new_secret(),
                pending_secret: None,
                pairing: None,
            },
        };
        if state.runner_id.is_some() {
            println!("runner_already_paired");
            return Ok(());
        }
        if state.pairing.as_ref().is_none_or(|p| p.expires_at <= now()) {
            let label = label.unwrap_or_else(|| "Nebula runner".into());
            let response=transport.request("v1/runner-pairings",None,&json!({"label":label,"version":env!("CARGO_PKG_VERSION"),"providers":super::PROVIDERS})).await.map_err(|e|e.code)?;
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Started {
                pairing_id: String,
                user_code: String,
                verification_uri: Option<String>,
                expires_in: u64,
                interval: u64,
            }
            let start: Started = decode(response)?;
            if start.interval == 0 || start.interval > start.expires_in {
                return Err("runner_response_invalid".into());
            }
            state.pairing = Some(Pairing {
                pairing_id: start.pairing_id,
                user_code: start.user_code,
                verification_uri: start.verification_uri,
                expires_at: now()
                    .checked_add(start.expires_in)
                    .ok_or("runner_response_invalid")?,
                interval: start.interval,
            });
            file.save(&state)?;
        }
        let pairing = state.pairing.as_ref().unwrap();
        println!(
            "Approve code {} at {}",
            pairing.user_code,
            pairing
                .verification_uri
                .as_deref()
                .unwrap_or(&state.endpoint)
        );
        loop {
            let pairing = state.pairing.as_ref().unwrap();
            if now() >= pairing.expires_at {
                return Err("runner_pairing_expired".into());
            }
            let response = transport
                .request(
                    &format!("v1/runner-pairings/{}/token", segment(&pairing.pairing_id)?),
                    None,
                    &json!({"secret":state.secret}),
                )
                .await;
            match response {
                Ok(value) if value.get("status").and_then(Value::as_str) == Some("pending") => {}
                Ok(value) => {
                    #[derive(Deserialize)]
                    #[serde(deny_unknown_fields)]
                    struct Delivered {
                        runner_id: String,
                    }
                    state.runner_id = Some(decode::<Delivered>(value)?.runner_id);
                    state.pairing = None;
                    file.save(&state)?;
                    println!("runner_paired");
                    return Ok(());
                }
                Err(error) if error.retryable => {}
                Err(error) => return Err(error.code),
            }
            tokio::time::sleep(Duration::from_secs(pairing.interval)).await;
        }
    }
    let mut state = file.read()?.ok_or("runner_not_paired")?;
    let runner_id = state.runner_id.clone().ok_or("runner_not_paired")?;
    let transport = Transport::new(&state.endpoint)?;
    let mut access = Zeroizing::new(String::new());
    let mut expires = Instant::now();
    let mut tasks = JoinSet::new();
    loop {
        while let Some(result) = tasks.try_join_next() {
            report(result);
        }
        if Instant::now() >= expires {
            if state.pending_secret.is_none() {
                state.pending_secret = Some(new_secret());
                file.save(&state)?;
            }
            let started = Instant::now();
            match transport
                .request(
                    "v1/runner-tokens",
                    None,
                    &json!({"runner_id":runner_id,"secret":state.secret,"replacement_secret":state.pending_secret}),
                )
                .await
            {
                Ok(value) => {
                    #[derive(Deserialize)]
                    #[serde(deny_unknown_fields)]
                    struct Token {
                        access_token: String,
                        expires_in: u64,
                    }
                    let mut token: Token = decode(value)?;
                    if token.expires_in > 300 {
                        return Err("runner_response_invalid".into());
                    }
                    state.secret.zeroize();
                    state.secret = state.pending_secret.take().unwrap();
                    file.save(&state)?;
                    *access = std::mem::take(&mut token.access_token);
                    expires = started + Duration::from_secs(token.expires_in.saturating_sub(40));
                    if Instant::now() >= expires {
                        continue;
                    }
                }
                Err(error) if error.retryable => {
                    tokio::time::sleep(error.retry_after).await;
                    continue;
                }
                Err(error) => return Err(error.code),
            }
        }
        let reply=transport.request(&format!("v1/runners/{}/work",segment(&runner_id)?),Some(&access),&json!({"protocol_version":1,"version":env!("CARGO_PKG_VERSION"),"providers":super::PROVIDERS,"limit":WORK_LIMIT-tasks.len(),"wait_seconds":30})).await;
        match reply {
            Ok(value) => {
                #[derive(Deserialize)]
                #[serde(deny_unknown_fields)]
                struct Work {
                    assignments: Vec<Assignment>,
                }
                let work: Work = decode(value)?;
                if work.assignments.len() > WORK_LIMIT - tasks.len() {
                    return Err("runner_response_invalid".into());
                }
                for assignment in work.assignments {
                    let endpoint = state.endpoint.clone();
                    tasks.spawn(async move { assignment.run(&endpoint).await });
                }
            }
            Err(error) if error.retryable => tokio::time::sleep(error.retry_after).await,
            Err(error) if error.unauthorized => {
                expires = Instant::now();
            }
            Err(error) => return Err(error.code),
        }
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Assignment {
    workspace_id: String,
    collection_id: String,
    connection_id: String,
    run_id: String,
    scan_id: String,
    complete_inventory: bool,
    reason: String,
    run_token: String,
}
impl Drop for Assignment {
    fn drop(&mut self) {
        self.run_token.zeroize();
    }
}
impl Assignment {
    async fn run(self, endpoint: &str) -> Result<(), String> {
        if self.reason != "triggered" {
            return Err("runner_assignment_invalid".into());
        }
        let client = Client::new(
            endpoint,
            &self.workspace_id,
            &self.collection_id,
            &self.connection_id,
            &self.run_token,
        )
        .map_err(|e| e.to_string())?;
        let connection = client.connection().await.map_err(|e| e.to_string())?;
        let connector = super::connector(&connection.provider)?;
        let completed = runner::run(
            connector,
            client,
            &self.run_id,
            &self.scan_id,
            self.complete_inventory,
        )
        .await
        .map_err(|e| e.to_string())?;
        println!(
            "{}",
            json!({"run_id":completed.id,"scan_id":completed.scan_id,"state":completed.state})
        );
        Ok(())
    }
}
fn report(result: Result<Result<(), String>, tokio::task::JoinError>) {
    match result {
        Ok(Ok(())) => {}
        Ok(Err(code)) => eprintln!("{code}"),
        Err(_) => eprintln!("runner_task_failed"),
    }
}
fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
fn segment(value: &str) -> Result<&str, String> {
    if value.is_empty()
        || value.len() > 256
        || !value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    {
        return Err("runner_response_invalid".into());
    }
    Ok(value)
}
fn new_secret() -> String {
    let mut bytes = [0u8; 32];
    bytes[..16].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
    bytes[16..].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
    let value = format!("nrs_{}", URL_SAFE_NO_PAD.encode(bytes));
    bytes.zeroize();
    value
}
fn decode<T: serde::de::DeserializeOwned>(value: Value) -> Result<T, String> {
    serde_json::from_value(value).map_err(|_| "runner_response_invalid".into())
}
struct Failure {
    code: String,
    retryable: bool,
    unauthorized: bool,
    retry_after: Duration,
}
impl Failure {
    fn transport() -> Self {
        Self {
            code: "runner_transport_failed".into(),
            retryable: true,
            unauthorized: false,
            retry_after: Duration::from_secs(5),
        }
    }
}
struct Transport {
    http: HttpClient,
    base: Url,
}
impl Transport {
    fn new(endpoint: &str) -> Result<Self, String> {
        Client::new(
            endpoint,
            "validation",
            "validation",
            "validation",
            "validation",
        )
        .map_err(|_| "runner_endpoint_invalid")?;
        let mut base = Url::parse(endpoint).map_err(|_| "runner_endpoint_invalid")?;
        if !base.path().ends_with('/') {
            base.set_path(&format!("{}/", base.path()));
        }
        let http = HttpClient::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(45))
            .build()
            .map_err(|_| "runner_transport_failed")?;
        Ok(Self { http, base })
    }
    async fn request(
        &self,
        path: &str,
        token: Option<&str>,
        body: &Value,
    ) -> Result<Value, Failure> {
        let url = self.base.join(path).map_err(|_| Failure::transport())?;
        let mut request = self.http.post(url).json(body);
        if let Some(token) = token {
            let mut header = HeaderValue::from_str(&format!("Bearer {token}"))
                .map_err(|_| Failure::transport())?;
            header.set_sensitive(true);
            request = request.header("Authorization", header);
        }
        let mut response = request.send().await.map_err(|_| Failure::transport())?;
        let status = response.status();
        if !status.is_success() {
            return Err(Failure {
                code: format!("runner_request_rejected_{}", status.as_u16()),
                retryable: status.is_server_error() || status.as_u16() == 429,
                unauthorized: matches!(status.as_u16(), 401 | 403),
                retry_after: Duration::from_secs(
                    response
                        .headers()
                        .get("Retry-After")
                        .and_then(|v| v.to_str().ok())
                        .and_then(|v| v.parse::<u64>().ok())
                        .unwrap_or(5)
                        .clamp(1, 60),
                ),
            });
        }
        let mut bytes = Zeroizing::new(Vec::new());
        while let Some(chunk) = response.chunk().await.map_err(|_| Failure::transport())? {
            if bytes.len() + chunk.len() > 65536 {
                return Err(Failure {
                    code: "runner_response_invalid".into(),
                    retryable: false,
                    unauthorized: false,
                    retry_after: Duration::ZERO,
                });
            }
            bytes.extend_from_slice(&chunk);
        }
        serde_json::from_slice(&bytes).map_err(|_| Failure {
            code: "runner_response_invalid".into(),
            retryable: false,
            unauthorized: false,
            retry_after: Duration::ZERO,
        })
    }
}
