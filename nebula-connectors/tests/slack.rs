use std::{collections::VecDeque, sync::Arc, time::Duration};

use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use nebula_connector_sdk::{
    Client, Connector, ConnectorError, Context, Run, ValidatedBatch, client::CredentialMode,
    runner, scope::Filter,
};
use nebula_connectors::slack::Slack;
use serde_json::{Value, json};
use tokio::sync::Mutex;

struct StateData {
    replies: VecDeque<Value>,
    requests: Vec<Value>,
    batches: Vec<Value>,
    run: Run,
    scope: Value,
}
struct Server {
    address: std::net::SocketAddr,
    state: Arc<Mutex<StateData>>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Server {
    async fn start(replies: Vec<Value>, scope: Value) -> Self {
        let state = Arc::new(Mutex::new(StateData {
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
    async fn context(&self) -> Context {
        let state = self.state.lock().await;
        Context {
            service: self.client(),
            run: state.run.clone(),
            config: json!({}),
            authority: "T1".into(),
            scope: Filter::parse(
                &serde_json::to_vec(&state.scope).unwrap(),
                true,
                &Slack.specification().capabilities.supported_filters,
            )
            .unwrap(),
            complete_inventory: true,
            credential_mode: CredentialMode::Managed,
        }
    }
}
async fn connection(State(state): State<Arc<Mutex<StateData>>>) -> Json<Value> {
    Json(
        json!({"id":"connection","tenant_id":"tenant","workspace_id":"workspace","collection_id":"collection",
        "provider":"slack","authority":"T1","config":{},"scope":state.lock().await.scope,
        "scope_revision":1,"credential_revision":1,"authorization_revision":1,"fence":1,
        "mode":"customer","credential_mode":"managed","state":"active","recurring":true}),
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
    if let Some(status) = body["http_status"].as_u64() {
        return (
            StatusCode::from_u16(status as u16).unwrap(),
            [("retry-after", body["retry_after"].as_str().unwrap())],
            Json(json!({"code":"backpressure"})),
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
fn message(ts: &str, text: &str) -> Value {
    json!({"type":"message","ts":ts,"text":text,"user":"U1"})
}
fn page(messages: Vec<Value>, next: &str) -> Value {
    json!({"ok":true,"messages":messages,"has_more":!next.is_empty(),"response_metadata":{"next_cursor":next}})
}
fn wire(page: &nebula_connector_sdk::Page) -> Vec<Value> {
    page.events
        .iter()
        .map(|e| serde_json::to_value(e).unwrap())
        .collect()
}

#[tokio::test]
async fn message_and_reply_inventory_resumes_at_the_accepted_page_after_reader_restart() {
    let mut root = message("1700000000.000001", "parent text");
    root["reply_count"] = 2.into();
    let mut reply1 = message("1700000001.000002", "first reply");
    reply1["thread_ts"] = "1700000000.000001".into();
    let mut reply2 = message("1700000002.000003", "second reply");
    reply2["thread_ts"] = "1700000000.000001".into();
    let server = Server::start(
        vec![
            json!({"ok":true,"team_id":"T1"}),
            page(
                vec![
                    root.clone(),
                    message("1700000003.000004", "separate message"),
                ],
                "older-history",
            ),
            page(vec![root, reply1], "more-replies"),
            json!({"ok":false,"error":"internal_error"}),
            json!({"ok":true,"team_id":"T1"}),
            page(vec![reply2], ""),
            page(vec![message("1600000000.000001", "old history")], ""),
        ],
        json!({"containers":["C1"]}),
    )
    .await;
    let first = tokio::time::timeout(
        Duration::from_secs(5),
        runner::run(&Slack, server.client(), "run", "scan", true),
    )
    .await
    .unwrap();
    assert!(matches!(
        first,
        Err(runner::Error::Connector(ConnectorError::Response))
    ));
    {
        let state = server.state.lock().await;
        assert_eq!(state.run.state, "running");
        assert_eq!(state.batches.len(), 2);
        assert!(!state.batches.iter().any(|b| {
            b["events"]
                .as_array()
                .unwrap()
                .iter()
                .any(|e| e["type"] == "snapshot_complete")
        }));
    }
    let completed = tokio::time::timeout(
        Duration::from_secs(5),
        runner::run(&Slack, server.client(), "run", "scan", true),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(completed.state, "sealed");
    let state = server.state.lock().await;
    assert!(state.replies.is_empty());
    let operations: Vec<_> = state
        .requests
        .iter()
        .map(|r| r["operation"].as_str().unwrap())
        .collect();
    assert_eq!(
        operations,
        [
            "auth.test",
            "conversations.history",
            "conversations.replies",
            "conversations.replies",
            "auth.test",
            "conversations.replies",
            "conversations.history"
        ]
    );
    assert_eq!(
        state.requests[3]["arguments"],
        state.requests[5]["arguments"]
    );
    assert_ne!(
        state.requests[3]["id"], state.requests[5]["id"],
        "physical requests must have fresh attempt IDs"
    );
    assert_eq!(
        state.requests[5]["arguments"],
        json!({"channel":"C1","limit":"15","ts":"1700000000.000001","cursor":"more-replies"})
    );
    assert_eq!(state.requests[6]["arguments"]["cursor"], "older-history");
    let events: Vec<_> = state
        .batches
        .iter()
        .flat_map(|b| b["events"].as_array().unwrap())
        .collect();
    let sources: Vec<_> = events
        .iter()
        .filter(|e| e["type"] == "upsert")
        .map(|e| e["source"]["object_id"].as_str().unwrap())
        .collect();
    assert_eq!(
        sources,
        [
            "C1:1700000000.000001",
            "C1:1700000003.000004",
            "C1:1700000001.000002",
            "C1:1700000002.000003",
            "C1:1600000000.000001"
        ]
    );
    assert_eq!(
        events
            .iter()
            .filter(|e| e["type"] == "snapshot_complete")
            .count(),
        1
    );
    for event in events.iter().filter(|e| e["type"] == "upsert") {
        assert_eq!(event["content"]["kind"], "structured");
        assert_eq!(event["permissions"]["state"], "unavailable");
        assert!(
            events
                .iter()
                .any(|observation| observation["type"] == "item_observed"
                    && observation["observation_id"] == event["observation_id"]
                    && observation["source"] == event["source"])
        );
    }
}

#[tokio::test]
async fn excluded_parent_still_enumerates_selected_replies_and_skipped_items_retain_presence() {
    let mut root = message("1700000000.000001", "excluded old parent");
    root["reply_count"] = 1.into();
    let mut reply = message("1700000002.000001", "new reply");
    reply["thread_ts"] = "1700000000.000001".into();
    let mut file = message("1700000003.000001", "file message");
    file["files"] = json!([{"id":"F1","url_private":"https://files.slack.com/fixture"}]);
    let server = Server::start(
        vec![
            page(
                vec![root.clone(), file, message("1700000004.000001", "")],
                "",
            ),
            page(vec![root, reply], ""),
        ],
        json!({"containers":["C1"],"created":{"from_ms":1700000001000_i64}}),
    )
    .await;
    let mut context = server.context().await;
    let first = Slack.read(&context, &json!({})).await.unwrap();
    assert!(!first.complete);
    let events = wire(&first);
    assert_eq!(
        events
            .iter()
            .map(|e| e["type"].as_str().unwrap())
            .collect::<Vec<_>>(),
        [
            "item_observed",
            "item_observed",
            "presence",
            "item_observed",
            "presence"
        ]
    );
    assert_eq!(events[0]["selection"], "excluded");
    assert_eq!(events[0]["reason"], "filtered");
    assert_eq!(events[1]["reason"], "unsupported");
    assert_eq!(events[3]["reason"], "empty");
    context.run.next_sequence += 1;
    let second = Slack.read(&context, &first.cursor).await.unwrap();
    assert!(second.complete);
    let events = wire(&second);
    assert_eq!(events.len(), 2);
    assert_eq!(events[1]["source"]["object_id"], "C1:1700000002.000001");
    assert_eq!(events[1]["metadata"]["thread_ts"], "1700000000.000001");
}

#[tokio::test]
async fn limited_history_and_broken_pagination_cannot_authorize_absence() {
    for response in [
        json!({"ok":true,"messages":[],"is_limited":true}),
        json!({"ok":true,"messages":[],"has_more":true}),
        json!({"ok":true,"messages":[],"response_metadata":{"next_cursor":false}}),
        page(
            vec![
                message("1700000000.000001", "one"),
                message("1700000000.000001", "duplicate"),
            ],
            "",
        ),
        page(vec![message("1700000000.00001", "bad timestamp")], ""),
    ] {
        let server = Server::start(vec![response], json!({"containers":["C1"]})).await;
        let result = Slack.read(&server.context().await, &json!({})).await;
        assert!(matches!(
            result,
            Err(ConnectorError::IncompleteInventory | ConnectorError::Response)
        ));
    }
    let server = Server::start(vec![page(vec![], "repeated")], json!({"containers":["C1"]})).await;
    assert!(matches!(
        Slack
            .read(&server.context().await, &json!({"history":"repeated"}))
            .await,
        Err(ConnectorError::IncompleteInventory)
    ));
}

#[tokio::test]
async fn authority_mismatch_and_self_managed_mode_stop_before_content_reads() {
    let server = Server::start(
        vec![json!({"ok":true,"team_id":"T2"})],
        json!({"containers":["C1"]}),
    )
    .await;
    let mut context = server.context().await;
    assert!(matches!(
        Slack.check(&context).await,
        Err(ConnectorError::Permission)
    ));
    context.credential_mode = CredentialMode::SelfManaged;
    assert!(matches!(
        Slack.check(&context).await,
        Err(ConnectorError::Configuration)
    ));
    assert!(matches!(
        Slack.read(&context, &json!({})).await,
        Err(ConnectorError::Configuration)
    ));
    assert_eq!(server.state.lock().await.requests.len(), 1);
}

#[tokio::test]
async fn message_revision_tracks_own_content_but_not_thread_reply_counts() {
    let original = message("1700000000.123456", "original");
    let mut added_reply = original.clone();
    added_reply["reply_count"] = 1.into();
    added_reply["latest_reply"] = "1700000001.000001".into();
    added_reply["thread_ts"] = "1700000000.123456".into();
    let mut edited = original.clone();
    edited["text"] = "edited".into();
    edited["edited"] = json!({"ts":"1700000002.654321","user":"U1"});
    let server = Server::start(
        vec![
            page(vec![original], ""),
            page(vec![added_reply], ""),
            page(vec![edited], ""),
        ],
        json!({"containers":["C1"]}),
    )
    .await;
    let context = server.context().await;
    let first = wire(&Slack.read(&context, &json!({})).await.unwrap());
    let second = wire(&Slack.read(&context, &json!({})).await.unwrap());
    let third = wire(&Slack.read(&context, &json!({})).await.unwrap());
    assert_eq!(
        first[1]["source"]["revision"],
        second[1]["source"]["revision"]
    );
    assert_ne!(
        first[1]["source"]["revision"],
        third[1]["source"]["revision"]
    );
    assert_eq!(third[0]["metadata"]["created_at_ms"], 1700000000123_i64);
    assert_eq!(third[0]["metadata"]["modified_at_ms"], 1700000002654_i64);
    assert_eq!(third[1]["content"]["value"]["text"], "edited");
}

#[tokio::test]
async fn discovery_reads_only_selected_channels_and_rejects_changed_channel_identity() {
    let server = Server::start(
        vec![
            json!({"ok":true,"channel":{"id":"C1","name":"selected"}}),
            json!({"ok":true,"channel":{"id":"C3","name":"wrong"}}),
        ],
        json!({"containers":["C1","G2"]}),
    )
    .await;
    let context = server.context().await;
    let first = Slack.discover(&context, None).await.unwrap();
    assert_eq!(first.resources.len(), 1);
    assert_eq!(first.resources[0].id, "C1");
    assert_eq!(first.resources[0].name, "selected");
    assert_eq!(first.next, Some(json!({"channel":1})));
    assert!(matches!(
        Slack.discover(&context, first.next.as_ref()).await,
        Err(ConnectorError::Response)
    ));
    let state = server.state.lock().await;
    assert_eq!(state.requests[0]["arguments"], json!({"channel":"C1"}));
    assert_eq!(state.requests[1]["arguments"], json!({"channel":"G2"}));
}

#[tokio::test]
async fn inventory_moves_to_the_next_channel_only_after_all_thread_pages() {
    let mut root = message("1700000000.000001", "parent");
    root["reply_count"] = 1.into();
    let mut reply = message("1700000001.000001", "reply");
    reply["thread_ts"] = "1700000000.000001".into();
    let server = Server::start(
        vec![
            page(vec![root.clone()], ""),
            page(vec![root, reply], ""),
            page(vec![], ""),
        ],
        json!({"containers":["C1","G2"]}),
    )
    .await;
    let mut context = server.context().await;
    let first = Slack.read(&context, &json!({})).await.unwrap();
    assert!(!first.complete);
    context.run.next_sequence += 1;
    let second = Slack.read(&context, &first.cursor).await.unwrap();
    assert!(!second.complete);
    context.run.next_sequence += 1;
    let third = Slack.read(&context, &second.cursor).await.unwrap();
    assert!(third.complete);
    let requests = &server.state.lock().await.requests;
    assert_eq!(requests[0]["arguments"]["channel"], "C1");
    assert_eq!(requests[1]["operation"], "conversations.replies");
    assert_eq!(requests[1]["arguments"]["channel"], "C1");
    assert_eq!(requests[2]["arguments"]["channel"], "G2");
    assert!(requests[2]["arguments"].get("cursor").is_none());
}

#[tokio::test]
async fn bounded_message_pages_fit_the_wire_budget_and_oversized_messages_are_explicit() {
    let messages: Vec<_> = (0..15)
        .map(|i| message(&format!("1700000000.{i:06}"), &"x".repeat(48_000)))
        .collect();
    let server = Server::start(
        vec![json!({"ok":true,"team_id":"T1"}), page(messages, "")],
        json!({"containers":["C1"]}),
    )
    .await;
    let completed = tokio::time::timeout(
        Duration::from_secs(5),
        runner::run(&Slack, server.client(), "run", "scan", true),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(completed.state, "sealed");
    let state = server.state.lock().await;
    assert_eq!(state.batches.len(), 1);
    assert_eq!(state.batches[0]["events"].as_array().unwrap().len(), 32);
    drop(state);
    let server = Server::start(
        vec![page(
            vec![message("1700000000.000001", &"x".repeat(50_000))],
            "",
        )],
        json!({"containers":["C1"]}),
    )
    .await;
    let page = Slack
        .read(&server.context().await, &json!({}))
        .await
        .unwrap();
    let events = wire(&page);
    assert_eq!(events[0]["outcome"], "skipped");
    assert_eq!(events[0]["reason"], "unsupported");
    assert_eq!(events[1]["type"], "presence");
    assert!(page.complete);
}

#[tokio::test]
async fn a_foreign_thread_or_failed_thread_read_cannot_seal_the_channel() {
    let mut foreign = message("1700000001.000001", "wrong thread");
    foreign["thread_ts"] = "1700000002.000001".into();
    for response in [
        page(vec![foreign], ""),
        json!({"ok":false,"error":"thread_not_found"}),
    ] {
        let server = Server::start(vec![response], json!({"containers":["C1"]})).await;
        let result = Slack
            .read(
                &server.context().await,
                &json!({"threads":["1700000000.000001"],"history_done":true}),
            )
            .await;
        assert!(matches!(result, Err(ConnectorError::Response)));
    }
}

#[tokio::test]
async fn quota_retry_uses_a_fresh_attempt_without_advancing_the_provider_cursor() {
    let server = Server::start(
        vec![
            json!({"http_status":429,"retry_after":"0"}),
            page(vec![message("1700000000.000001", "after quota wait")], ""),
        ],
        json!({"containers":["C1"]}),
    )
    .await;
    let result = Slack
        .read(
            &server.context().await,
            &json!({"history":"accepted-cursor"}),
        )
        .await
        .unwrap();
    assert!(result.complete);
    let state = server.state.lock().await;
    assert_eq!(state.requests.len(), 2);
    assert_ne!(state.requests[0]["id"], state.requests[1]["id"]);
    assert_eq!(
        state.requests[0]["arguments"],
        state.requests[1]["arguments"]
    );
    assert_eq!(state.requests[1]["arguments"]["cursor"], "accepted-cursor");
    drop(state);
    let server = Server::start(
        vec![json!({"http_status":429,"retry_after":"0"}); 3],
        json!({"containers":["C1"]}),
    )
    .await;
    assert!(matches!(
        Slack.read(&server.context().await, &json!({})).await,
        Err(ConnectorError::Transport(
            nebula_connector_sdk::ClientError::Rejected { status: 429, .. }
        ))
    ));
    assert_eq!(server.state.lock().await.requests.len(), 3);
    let server = Server::start(
        vec![json!({"http_status":429,"retry_after":"86400"})],
        json!({"containers":["C1"]}),
    )
    .await;
    assert!(matches!(
        Slack.read(&server.context().await, &json!({})).await,
        Err(ConnectorError::Transport(
            nebula_connector_sdk::ClientError::Rejected { status: 429, .. }
        ))
    ));
    assert_eq!(server.state.lock().await.requests.len(), 1);
}

struct RunFile(std::path::PathBuf);
impl Drop for RunFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}
impl RunFile {
    fn create(bytes: &[u8]) -> Self {
        use std::io::Write;
        let path =
            std::env::temp_dir().join(format!("connector-run-{}.json", uuid::Uuid::new_v4()));
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&path).unwrap();
        let guard = Self(path);
        file.write_all(bytes).unwrap();
        guard
    }
}

async fn command(command: &str, config: &RunFile) -> std::process::Output {
    struct Child(Option<std::process::Child>);
    impl Drop for Child {
        fn drop(&mut self) {
            if let Some(child) = &mut self.0 {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
    }
    let mut child = Child(Some(
        std::process::Command::new(env!("CARGO_BIN_EXE_nebula-connector"))
            .args(["slack", command])
            .env("CONNECTOR_RUN_FILE", &config.0)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .unwrap(),
    ));
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if child.0.as_mut().unwrap().try_wait().unwrap().is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("runner process exited");
    child.0.take().unwrap().wait_with_output().unwrap()
}

#[tokio::test]
async fn runner_executable_uses_a_secret_file_and_prints_only_completion_identity() {
    let server = Server::start(
        vec![
            json!({"ok":true,"team_id":"T1"}),
            page(
                vec![message("1700000000.000001", "private message body")],
                "",
            ),
        ],
        json!({"containers":["C1"]}),
    )
    .await;
    let config = RunFile::create(&serde_json::to_vec(&json!({"endpoint":format!("http://{}",server.address),
        "workspace_id":"workspace","collection_id":"collection","connection_id":"connection",
        "run_id":"run","scan_id":"scan","complete_inventory":true,"bearer":"private-nebula-bearer"})).unwrap());
    let output = command("run", &config).await;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stderr.is_empty());
    assert_eq!(
        serde_json::from_slice::<Value>(&output.stdout).unwrap(),
        json!({"run_id":"run","scan_id":"scan","state":"sealed"})
    );
    assert_eq!(server.state.lock().await.batches.len(), 1);
}

#[tokio::test]
async fn runner_executable_reports_capabilities_and_keeps_invalid_configuration_secret() {
    let config = RunFile::create(br#"{"bearer":"private-value","bearer":"other-value"}"#);
    let output = command("spec", &config).await;
    assert!(output.status.success());
    let spec: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(spec["provider"], "slack");
    assert_eq!(spec["capabilities"]["complete_inventory"], true);
    assert_eq!(spec["capabilities"]["incremental"], false);
    let output = command("run", &config).await;
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert_eq!(output.stderr, b"connector_run_configuration_invalid\n");
}
