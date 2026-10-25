use std::{
    io::Write,
    net::TcpListener,
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use async_trait::async_trait;
use nebula_connector_sdk::{
    Client, Connector, ConnectorError, Context, Page, Specification,
    connector::{Capabilities, ResourcePage},
    runner,
};
use serde_json::{Value, json};

mod support;
use support::{accept, request, respond, run};

struct Reader {
    cursors: Arc<Mutex<Vec<(bool, Value)>>>,
    block: Option<Arc<tokio::sync::Notify>>,
    invalid: bool,
}

#[async_trait]
impl Connector for Reader {
    fn specification(&self) -> Specification {
        Specification {
            provider: "test".into(),
            version: "1".into(),
            cursor_version: 1,
            config_schema: json!({"type":"object"}),
            capabilities: Capabilities {
                managed: true,
                incremental: true,
                complete_inventory: true,
                permissions: false,
                original_files: false,
                supported_filters: vec!["container".into()],
            },
        }
    }
    async fn check(&self, context: &Context) -> Result<(), ConnectorError> {
        assert_eq!(context.authority, "account");
        assert_eq!(context.scope.containers(), ["folder"]);
        assert_eq!(context.config, json!({"setting":true}));
        Ok(())
    }
    async fn discover(
        &self,
        _: &Context,
        _: Option<&Value>,
    ) -> Result<ResourcePage, ConnectorError> {
        unreachable!()
    }
    async fn read(&self, context: &Context, cursor: &Value) -> Result<Page, ConnectorError> {
        self.cursors
            .lock()
            .unwrap()
            .push((context.complete_inventory, cursor.clone()));
        if let Some(block) = &self.block {
            block.notified().await;
        }
        let source = json!({"authority":"account","object_id":"item","revision":"v2"});
        let events = if self.invalid {
            vec![serde_json::from_value(json!({"type":"checkpoint","cursor":{}})).unwrap()]
        } else {
            vec![
                serde_json::from_value(json!({"type":"item_observed","observation_id":"obs","scan_id":context.run.scan_id,"source":source,"observed_at_ms":1,"metadata":{"container_id":"folder"},"selection":"selected","outcome":"observed","reason":"in_scope"})).unwrap(),
                serde_json::from_value(json!({"type":"presence","change_id":"change","observation_id":"obs","source":source,"observed_at_ms":1})).unwrap(),
            ]
        };
        Ok(Page {
            events,
            cursor: json!({"next":"end"}),
            complete: true,
        })
    }
}

fn connection() -> Value {
    json!({"id":"connection","tenant_id":"tenant","workspace_id":"workspace","collection_id":"collection","provider":"test","authority":"account","config":{"setting":true},"scope":{"containers":["folder"]},"scope_revision":1,"credential_revision":1,"authorization_revision":1,"fence":1,"mode":"customer","credential_mode":"managed","state":"active","recurring":true})
}

fn client(listener: &TcpListener) -> Client {
    Client::new(
        &format!("http://{}", listener.local_addr().unwrap()),
        "workspace",
        "collection",
        "connection",
        "nebula-grant",
    )
    .unwrap()
}

fn respond_start(listener: &TcpListener, run: &nebula_connector_sdk::Run) {
    let (mut stream, _) = accept(listener);
    let (path, _) = request(&mut stream);
    assert!(path.starts_with("GET "));
    respond(&mut stream, 200, &connection().to_string());
    let (mut stream, _) = accept(listener);
    let (path, _) = request(&mut stream);
    assert!(path.contains("/runs "));
    respond(&mut stream, 200, &serde_json::to_string(run).unwrap());
}

#[tokio::test]
async fn invalid_scope_stops_before_run_admission_and_provider_work() {
    for scope in [
        json!({"containers":[]}),
        json!({"containers":["folder","folder"]}),
        json!({"containers":["folder"],"unknown":true}),
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let service = client(&listener);
        let seen = Arc::new(Mutex::new(Vec::new()));
        let reader = Reader {
            cursors: seen.clone(),
            block: None,
            invalid: false,
        };
        let server = thread::spawn(move || {
            let (mut stream, _) = accept(&listener);
            let (path, _) = request(&mut stream);
            assert!(path.starts_with("GET "));
            let mut value = connection();
            value["scope"] = scope;
            respond(&mut stream, 200, &value.to_string());
        });
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            runner::run(&reader, service, "run", "scan", true),
        )
        .await
        .unwrap();
        server.join().unwrap();
        assert!(matches!(result, Err(runner::Error::Configuration)));
        assert!(seen.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn lost_acceptance_reply_replays_identical_batch_and_seals_after_events() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let client = client(&listener);
    let seen = Arc::new(Mutex::new(Vec::new()));
    let reader = Reader {
        cursors: seen.clone(),
        block: None,
        invalid: false,
    };
    let server = thread::spawn(move || {
        let mut resumed = run();
        resumed.next_sequence = 4;
        resumed.cursor = json!({"version":1,"value":{"next":"resume"}});
        respond_start(&listener, &resumed);
        let (mut lost, _) = accept(&listener);
        let (_, accepted) = request(&mut lost);
        drop(lost);
        let (mut retry, _) = accept(&listener);
        let (path, replay) = request(&mut retry);
        assert!(path.contains("/batches "));
        assert_eq!(replay, accepted, "uncertain batch was rebuilt or refetched");
        let batch: Value = serde_json::from_slice(&replay).unwrap();
        assert_eq!(batch["sequence"], 4);
        let types: Vec<_> = batch["events"]
            .as_array()
            .unwrap()
            .iter()
            .map(|e| e["type"].as_str().unwrap())
            .collect();
        assert_eq!(
            types,
            [
                "item_observed",
                "presence",
                "snapshot_complete",
                "checkpoint"
            ]
        );
        assert_eq!(
            batch["events"][3]["cursor"],
            json!({"version":1,"value":{"next":"end"}})
        );
        respond(&mut retry,200,&json!({"batch_id":batch["batch_id"],"fence":1,"sequence":4,"sha256":"a".repeat(64),"changes":["change"]}).to_string());
    });
    let finished = tokio::time::timeout(
        Duration::from_secs(10),
        runner::run(&reader, client, "run", "scan", true),
    )
    .await
    .unwrap()
    .unwrap();
    server.join().unwrap();
    assert_eq!(finished.state, "sealed");
    assert_eq!(finished.next_sequence, 5);
    assert_eq!(
        *seen.lock().unwrap(),
        vec![(true, json!({"next":"resume"}))]
    );
}

#[tokio::test]
async fn incremental_completion_does_not_emit_an_inventory_seal_and_restart_does_not_read() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let client = client(&listener);
    let seen = Arc::new(Mutex::new(Vec::new()));
    let reader = Reader {
        cursors: seen.clone(),
        block: None,
        invalid: false,
    };
    let server = thread::spawn(move || {
        respond_start(&listener, &run());
        let (mut stream, _) = accept(&listener);
        let (_, body) = request(&mut stream);
        let batch: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(batch["events"].as_array().unwrap().len(), 3);
        assert_eq!(batch["events"][2]["type"], "checkpoint");
        respond(&mut stream,200,&json!({"batch_id":batch["batch_id"],"fence":1,"sequence":0,"sha256":"a".repeat(64),"changes":["change"]}).to_string());
        let (mut stream, _) = accept(&listener);
        let (path, body) = request(&mut stream);
        assert!(path.contains("/complete "));
        assert_eq!(
            serde_json::from_slice::<Value>(&body).unwrap(),
            json!({"fence":1,"next_sequence":1})
        );
        respond(&mut stream, 200, r#"{"state":"sealed"}"#);
        let mut completed = run();
        completed.state = "sealed".into();
        completed.next_sequence = 1;
        completed.lease_remaining_ms = 0;
        completed.cursor = json!({"version":1,"value":{"next":"end"}});
        respond_start(&listener, &completed);
    });
    for _ in 0..2 {
        let finished = tokio::time::timeout(
            Duration::from_secs(10),
            runner::run(&reader, client.clone(), "run", "scan", false),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(finished.state, "sealed");
    }
    server.join().unwrap();
    assert_eq!(*seen.lock().unwrap(), vec![(false, json!({}))]);
}

#[tokio::test]
async fn lease_renewal_runs_during_blocked_provider_read_and_fencing_cancels_it() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let client = client(&listener);
    let seen = Arc::new(Mutex::new(Vec::new()));
    let reader = Reader {
        cursors: seen.clone(),
        block: Some(Arc::new(tokio::sync::Notify::new())),
        invalid: false,
    };
    let server = thread::spawn(move || {
        let mut initial = run();
        initial.lease_remaining_ms = 3000;
        respond_start(&listener, &initial);
        let (mut stream, _) = accept(&listener);
        let (path, body) = request(&mut stream);
        assert!(path.contains("/renew "));
        assert_eq!(
            serde_json::from_slice::<Value>(&body).unwrap(),
            json!({"fence":1})
        );
        respond(&mut stream, 409, r#"{"code":"stale_fence"}"#);
        let (mut stream, _) = accept(&listener);
        let (path, _) = request(&mut stream);
        assert!(path.contains("/runs "));
        respond(&mut stream, 409, r#"{"code":"stale_fence"}"#);
    });
    let result = tokio::time::timeout(
        Duration::from_secs(10),
        runner::run(&reader, client, "run", "scan", true),
    )
    .await
    .unwrap();
    server.join().unwrap();
    assert!(matches!(result, Err(runner::Error::LeaseLost)));
    assert_eq!(seen.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn mismatched_cursor_version_stops_before_provider_work() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let client = client(&listener);
    let seen = Arc::new(Mutex::new(Vec::new()));
    let reader = Reader {
        cursors: seen.clone(),
        block: None,
        invalid: false,
    };
    let server = thread::spawn(move || {
        let mut initial = run();
        initial.cursor = json!({"version":2,"value":{"next":"opaque"}});
        respond_start(&listener, &initial);
    });
    let result = runner::run(&reader, client, "run", "scan", true).await;
    server.join().unwrap();
    assert!(matches!(result, Err(runner::Error::CursorVersion)));
    assert!(seen.lock().unwrap().is_empty());
}

#[tokio::test]
async fn adapter_cannot_inject_checkpoint_or_inventory_completion() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let client = client(&listener);
    let reader = Reader {
        cursors: Arc::new(Mutex::new(Vec::new())),
        block: None,
        invalid: true,
    };
    let server = thread::spawn(move || respond_start(&listener, &run()));
    let result = runner::run(&reader, client, "run", "scan", true).await;
    server.join().unwrap();
    assert!(matches!(result, Err(runner::Error::Page)));
}

#[tokio::test]
async fn final_commit_reconciles_when_renewal_races_a_lost_response() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let client = client(&listener);
    let seen = Arc::new(Mutex::new(Vec::new()));
    let reader = Reader {
        cursors: seen.clone(),
        block: None,
        invalid: false,
    };
    let (finished, received) = std::sync::mpsc::channel();
    let server = thread::spawn(move || {
        let mut initial = run();
        initial.lease_remaining_ms = 3000;
        respond_start(&listener, &initial);
        let (mut pending, _) = accept(&listener);
        let (path, body) = request(&mut pending);
        assert!(path.contains("/batches "));
        let batch: Value = serde_json::from_slice(&body).unwrap();
        let (mut stream, _) = accept(&listener);
        let (path, _) = request(&mut stream);
        assert!(path.contains("/renew "));
        respond(&mut stream, 409, r#"{"code":"stale_fence"}"#);
        let (mut stream, _) = accept(&listener);
        let (path, _) = request(&mut stream);
        assert!(path.contains("/runs "));
        let mut completed = run();
        completed.state = "sealed".into();
        completed.next_sequence = 1;
        completed.lease_remaining_ms = 0;
        completed.cursor = batch["events"][3]["cursor"].clone();
        respond(
            &mut stream,
            200,
            &serde_json::to_string(&completed).unwrap(),
        );
        received.recv_timeout(Duration::from_secs(5)).unwrap();
        drop(pending);
    });
    let result = tokio::time::timeout(
        Duration::from_secs(10),
        runner::run(&reader, client, "run", "scan", true),
    )
    .await
    .unwrap();
    finished.send(()).unwrap();
    server.join().unwrap();
    let result = result.unwrap();
    assert_eq!(result.state, "sealed");
    assert_eq!(result.cursor, json!({"version":1,"value":{"next":"end"}}));
    assert_eq!(seen.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn temporary_renewal_overload_does_not_restart_the_provider_page() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let client = client(&listener);
    let seen = Arc::new(Mutex::new(Vec::new()));
    let unblock = Arc::new(tokio::sync::Notify::new());
    let reader = Reader {
        cursors: seen.clone(),
        block: Some(unblock.clone()),
        invalid: false,
    };
    let server = thread::spawn(move || {
        let mut initial = run();
        initial.lease_remaining_ms = 3000;
        respond_start(&listener, &initial);
        let (mut stream, _) = accept(&listener);
        let (path, _) = request(&mut stream);
        assert!(path.contains("/renew "));
        write!(
            stream,
            "HTTP/1.1 503 Busy\r\nRetry-After: 0\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
        )
        .unwrap();
        let (mut stream, _) = accept(&listener);
        let (path, _) = request(&mut stream);
        assert!(path.contains("/renew "));
        respond(
            &mut stream,
            200,
            r#"{"lease_until":"2099-01-01T00:00:00Z","lease_remaining_ms":120000}"#,
        );
        unblock.notify_one();
        let (mut stream, _) = accept(&listener);
        let (path, body) = request(&mut stream);
        assert!(path.contains("/batches "));
        let batch: Value = serde_json::from_slice(&body).unwrap();
        respond(&mut stream,200,&json!({"batch_id":batch["batch_id"],"fence":1,"sequence":0,"sha256":"a".repeat(64),"changes":["change"]}).to_string());
    });
    let result = tokio::time::timeout(
        Duration::from_secs(10),
        runner::run(&reader, client, "run", "scan", true),
    )
    .await
    .unwrap()
    .unwrap();
    server.join().unwrap();
    assert_eq!(result.state, "sealed");
    assert_eq!(seen.lock().unwrap().len(), 1);
}
