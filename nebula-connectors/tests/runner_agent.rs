use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post},
};
use serde_json::{Value, json};
use std::{
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::Arc,
    time::Duration,
};
use tokio::sync::Mutex;

struct Process(Option<Child>);
impl Drop for Process {
    fn drop(&mut self) {
        if let Some(child) = &mut self.0 {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}
impl Process {
    async fn finish(mut self) -> std::process::Output {
        tokio::time::timeout(Duration::from_secs(10), async {
            while self.0.as_mut().unwrap().try_wait().unwrap().is_none() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("runner exited");
        self.0.take().unwrap().wait_with_output().unwrap()
    }
}
struct Fixture {
    address: String,
    path: PathBuf,
    state: Arc<Mutex<Observed>>,
    task: tokio::task::JoinHandle<()>,
}
struct Observed {
    path: PathBuf,
    pairing_secret: Option<String>,
    polls: usize,
    tokens: Vec<(String, String)>,
    token_attempts: usize,
    runs: Vec<String>,
    assigned: bool,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.task.abort();
        let _ = std::fs::remove_dir_all(self.path.parent().unwrap());
    }
}
impl Fixture {
    async fn new() -> Self {
        let path = std::env::temp_dir()
            .join(format!("runner-agent-{}", uuid::Uuid::new_v4()))
            .join("state.json");
        let state = Arc::new(Mutex::new(Observed {
            path: path.clone(),
            pairing_secret: None,
            polls: 0,
            tokens: vec![],
            token_attempts: 0,
            runs: vec![],
            assigned: false,
        }));
        let router=Router::new().route("/v1/runner-pairings",post(||async{Json(json!({"pairing_id":"rp_one","user_code":"BCDF-GHJK","expires_in":600,"interval":1}))}))
        .route("/v1/runner-pairings/{pairing}/token",post(pair))
        .route("/v1/runner-tokens",post(token))
        .route("/v1/runners/{runner}/work",post(work))
        .route("/v1/workspaces/w/collections/c/connections/{connection}",get(connection))
        .route("/v1/workspaces/w/collections/c/connections/{connection}/runs",post(begin))
        .with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        Self {
            address,
            path,
            state,
            task,
        }
    }
    fn command(&self, command: &str) -> Process {
        let mut process = Command::new(env!("CARGO_BIN_EXE_nebula-connector"));
        process.arg(command).arg("--state-file").arg(&self.path);
        if command == "pair" {
            process.arg("--endpoint").arg(&self.address);
        }
        Process(Some(
            process
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .unwrap(),
        ))
    }
}
async fn pair(
    State(state): State<Arc<Mutex<Observed>>>,
    Json(body): Json<Value>,
) -> (StatusCode, Json<Value>) {
    let mut state = state.lock().await;
    let secret = body["secret"].as_str().unwrap().to_owned();
    let saved: Value = serde_json::from_slice(&std::fs::read(&state.path).unwrap()).unwrap();
    assert_eq!(saved["secret"], secret, "bootstrap saved before dispatch");
    if let Some(previous) = &state.pairing_secret {
        assert_eq!(previous, &secret);
    } else {
        state.pairing_secret = Some(secret);
    }
    state.polls += 1;
    if state.polls == 1 {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error":"lost_response"})),
        );
    }
    (StatusCode::OK, Json(json!({"runner_id":"runner_one"})))
}
async fn token(
    State(state): State<Arc<Mutex<Observed>>>,
    Json(body): Json<Value>,
) -> (StatusCode, Json<Value>) {
    let mut state = state.lock().await;
    let secret = body["secret"].as_str().unwrap().to_owned();
    let proposal = body["replacement_secret"].as_str().unwrap().to_owned();
    let saved: Value = serde_json::from_slice(&std::fs::read(&state.path).unwrap()).unwrap();
    assert_eq!(saved["secret"], secret);
    assert_eq!(
        saved["pending_secret"], proposal,
        "replacement saved before dispatch"
    );
    state.token_attempts += 1;
    if let Some((old, next)) = state.tokens.last() {
        if old == &secret {
            assert_eq!(next, &proposal, "retry keeps the accepted proposal");
        } else {
            assert_eq!(next, &secret, "restart uses acknowledged replacement");
            state.tokens.push((secret, proposal));
        }
    } else {
        assert_eq!(Some(&secret), state.pairing_secret.as_ref());
        state.tokens.push((secret, proposal));
    }
    if state.token_attempts == 1 {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error":"lost_token_response"})),
        );
    }
    let expires_in = if state.token_attempts == 2 { 0 } else { 300 };
    (
        StatusCode::OK,
        Json(json!({"access_token":"nra_poll","expires_in":expires_in})),
    )
}
async fn work(State(state): State<Arc<Mutex<Observed>>>, Json(body): Json<Value>) -> Json<Value> {
    assert_eq!(body["protocol_version"], 1);
    let mut state = state.lock().await;
    if !state.assigned {
        state.assigned = true;
        return Json(
            json!({"assignments":(["one","two"].into_iter().map(|connection|json!({"workspace_id":"w","collection_id":"c","connection_id":connection,"run_id":format!("run_{connection}"),"scan_id":format!("scan_{connection}"),"complete_inventory":true,"reason":"triggered","run_token":"nrg_private"})).collect::<Vec<_>>())}),
        );
    }
    drop(state);
    tokio::time::sleep(Duration::from_secs(30)).await;
    Json(json!({"assignments":[]}))
}
async fn connection(Path(id): Path<String>) -> Json<Value> {
    Json(
        json!({"id":id,"tenant_id":"t","workspace_id":"w","collection_id":"c","provider":"slack","authority":"T1","config":{},"scope":{"containers":["C1"]},"scope_revision":1,"credential_revision":1,"authorization_revision":1,"fence":1,"mode":"customer","credential_mode":"managed","state":"active","recurring":false}),
    )
}
async fn begin(State(state): State<Arc<Mutex<Observed>>>, Json(body): Json<Value>) -> Json<Value> {
    state
        .lock()
        .await
        .runs
        .push(body["id"].as_str().unwrap().to_owned());
    Json(
        json!({"limits":{"max_batch_events":100,"max_batch_bytes":1048576,"max_cursor_bytes":65536},"id":body["id"],"scan_id":body["scan_id"],"fence":1,"scope_revision":1,"credential_revision":1,"authorization_revision":1,"lease_until":"2099-01-01T00:00:00Z","lease_remaining_ms":120000,"next_sequence":1,"cursor":{},"state":"sealed"}),
    )
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn executable_recovers_pairing_and_rotation_and_runs_multiple_connections() {
    let fixture = Fixture::new().await;
    let output = fixture.command("pair").finish().await;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("runner_paired"));
    let saved: Value = serde_json::from_slice(&std::fs::read(&fixture.path).unwrap()).unwrap();
    assert_eq!(saved["runner_id"], "runner_one");
    assert!(!String::from_utf8_lossy(&output.stdout).contains(saved["secret"].as_str().unwrap()));
    let first = fixture.command("serve");
    tokio::time::timeout(Duration::from_secs(10), async {
        while fixture.state.lock().await.runs.len() != 2 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("both connections ran");
    let duplicate = fixture.command("serve").finish().await;
    assert!(!duplicate.status.success());
    assert_eq!(duplicate.stderr, b"runner_already_running\n");
    drop(first);
    let second = fixture.command("serve");
    tokio::time::timeout(Duration::from_secs(10), async {
        while fixture.state.lock().await.tokens.len() != 3 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("restart exchanged persisted replacement");
    drop(second);
    let mut runs = fixture.state.lock().await.runs.clone();
    runs.sort();
    assert_eq!(runs, ["run_one", "run_two"]);
}
