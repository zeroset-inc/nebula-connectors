//! A stand-in Nebula service for provider adapter tests.
// Each test crate compiles this module and uses part of it.
#![allow(dead_code)]

use std::{collections::VecDeque, sync::Arc, time::Duration};

use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use nebula_connector_sdk::{Client, Connector, Run, ValidatedBatch, runner};
use serde_json::{Value, json};
use tokio::sync::Mutex;

pub struct StateData {
    provider: &'static str,
    authority: &'static str,
    replies: VecDeque<Value>,
    requests: Vec<Value>,
    batches: Vec<Value>,
    run: Run,
    scope: Value,
}

/// A stand-in Nebula service. Each provider request takes the next reply; a
/// reply with `provider_status` becomes the broker's provider failure.
pub struct Server {
    address: std::net::SocketAddr,
    pub state: Arc<Mutex<StateData>>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Server {
    pub async fn start(connector: &'static str, replies: Vec<Value>, scope: Value) -> Self {
        Self::start_as(connector, "1077", replies, scope).await
    }

    /// Starts the service for a connection signed in as `authority`.
    pub async fn start_as(
        connector: &'static str,
        authority: &'static str,
        replies: Vec<Value>,
        scope: Value,
    ) -> Self {
        let state = Arc::new(Mutex::new(StateData {
            provider: connector,
            authority,
            replies: replies.into(),
            requests: vec![],
            batches: vec![],
            run: Run {
                limits: nebula_connector_sdk::client::ProtocolLimits::V1,
                id: "run".into(),
                scan_id: "scan".into(),
                fence: 1,
                scope_revision: 1,
                credential_revision: 1,
                authorization_revision: 1,
                lease_until: "2099-01-01T00:00:00Z".into(),
                lease_remaining_ms: 120000,
                next_sequence: 0,
                cursor: json!({}),
                state: "running".into(),
            },
            scope,
        }));
        let base = "/v1/workspaces/workspace/collections/collection/connections/connection";
        let router = Router::new()
            .route(base, get(connection))
            .route(&format!("{base}/runs"), post(begin))
            .route(&format!("{base}/runs/run/requests"), post(provider))
            .route(&format!("{base}/runs/run/batches"), post(accept))
            .with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        Self {
            address,
            state,
            task,
        }
    }

    fn client(&self) -> Client {
        Client::new(
            &format!("http://{}", self.address),
            "workspace",
            "collection",
            "connection",
            "test-only-nebula-credential",
        )
        .unwrap()
    }

    pub async fn run(&self, connector: &dyn Connector) -> Result<Run, runner::Error> {
        tokio::time::timeout(
            Duration::from_secs(5),
            runner::run(connector, self.client(), "run", "scan", true),
        )
        .await
        .unwrap()
    }

    pub async fn events(&self) -> Vec<Value> {
        let state = self.state.lock().await;
        state
            .batches
            .iter()
            .flat_map(|b| b["events"].as_array().unwrap().clone())
            .collect()
    }

    pub async fn operations(&self) -> Vec<(String, Value)> {
        let state = self.state.lock().await;
        state
            .requests
            .iter()
            .map(|r| {
                (
                    r["operation"].as_str().unwrap().to_owned(),
                    r["arguments"].clone(),
                )
            })
            .collect()
    }
}

async fn connection(State(state): State<Arc<Mutex<StateData>>>) -> Json<Value> {
    Json(
        json!({"id":"connection","tenant_id":"tenant","workspace_id":"workspace","collection_id":"collection",
        "provider":state.lock().await.provider,"authority":state.lock().await.authority,"config":{},"scope":state.lock().await.scope,
        "scope_revision":1,"credential_revision":1,"authorization_revision":1,"fence":1,
        "mode":"hosted","credential_mode":"managed","state":"active","recurring":true}),
    )
}

async fn begin(State(state): State<Arc<Mutex<StateData>>>) -> Json<Run> {
    Json(state.lock().await.run.clone())
}

async fn provider(
    State(state): State<Arc<Mutex<StateData>>>,
    Json(request): Json<Value>,
) -> axum::response::Response {
    let mut state = state.lock().await;
    state.requests.push(request.clone());
    let Some(body) = state.replies.pop_front() else {
        return StatusCode::BAD_GATEWAY.into_response();
    };
    if let Some(status) = body["provider_status"].as_u64() {
        return (
            StatusCode::BAD_GATEWAY,
            Json(
                json!({"attempt_id":request["id"],"provider_status":status,"code":body["code"],
                "retryable":false,"retry_after_seconds":0}),
            ),
        )
            .into_response();
    }
    Json(
        json!({"attempt_id":request["id"],"status":200,"sha256":"0".repeat(64),
        "byte_length":serde_json::to_vec(&body).unwrap().len(),"body":body}),
    )
    .into_response()
}

async fn accept(
    State(state): State<Arc<Mutex<StateData>>>,
    Json(batch): Json<Value>,
) -> axum::response::Response {
    if ValidatedBatch::parse(&serde_json::to_vec(&batch).unwrap()).is_err() {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let mut state = state.lock().await;
    let events = batch["events"].as_array().unwrap();
    state.run.cursor = events.last().unwrap()["cursor"].clone();
    state.run.next_sequence += 1;
    if events.iter().any(|e| e["type"] == "snapshot_complete") {
        state.run.state = "sealed".into();
    }
    state.batches.push(batch.clone());
    Json(json!({"batch_id":batch["batch_id"],"sequence":batch["sequence"],"fence":batch["fence"],
        "sha256":"0".repeat(64),"changes":events.iter().filter_map(|e|e.get("change_id")).collect::<Vec<_>>()})).into_response()
}

pub fn of_object<'a>(events: &'a [Value], object: &str) -> Vec<&'a Value> {
    events
        .iter()
        .filter(|e| e["source"]["object_id"] == object)
        .collect()
}
