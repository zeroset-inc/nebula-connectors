use nebula_connector_sdk::{Client, ClientError, client::ProviderFailureCode};
use serde_json::json;
use std::{
    io::Write,
    net::TcpListener,
    sync::{Arc, Condvar, Mutex, mpsc},
    thread,
    time::{Duration, Instant},
};

mod support;
use support::{accept, request, respond, run};

#[tokio::test]
async fn provider_errors_keep_status_and_backoff_without_provider_bodies() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        let (mut stream, _) = accept(&listener);
        let (path, body) = request(&mut stream);
        assert!(path.contains("/runs/run/requests"));
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["id"], "attempt");
        respond(
            &mut stream,
            429,
            r#"{"attempt_id":"attempt","provider_status":429,"code":"provider_rejected","retryable":true,"retry_after_seconds":60}"#,
        );
    });
    let client = Client::new(
        &format!("http://{address}"),
        "workspace",
        "collection",
        "connection",
        "nebula-grant",
    )
    .unwrap();
    let error = client
        .provider(&run(), "attempt", "read", json!({}))
        .await
        .err()
        .unwrap();
    match error {
        ClientError::Provider(f) => {
            assert_eq!(f.provider_status, 429);
            assert_eq!(f.retry_after_seconds, 60);
            assert!(f.retryable);
            assert!(matches!(f.code, ProviderFailureCode::ProviderRejected));
        }
        _ => panic!("failure classification lost"),
    }
    server.join().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slow_provider_calls_leave_renewal_capacity() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let gate = Arc::new((Mutex::new(false), Condvar::new()));
    let release = gate.clone();
    let _cleanup = ReleaseGate(gate.clone());
    let (entered, received) = mpsc::channel();
    let server = thread::spawn(move || {
        let mut handlers = Vec::new();
        listener.set_nonblocking(true).unwrap();
        let deadline = Instant::now() + Duration::from_secs(8);
        for _ in 0..5 {
            let (mut stream, _) = loop {
                match listener.accept() {
                    Ok(connection) => break connection,
                    Err(e)
                        if e.kind() == std::io::ErrorKind::WouldBlock
                            && Instant::now() < deadline =>
                    {
                        thread::sleep(Duration::from_millis(5))
                    }
                    Err(_) => return,
                }
            };
            let gate = gate.clone();
            let entered = entered.clone();
            handlers.push(thread::spawn(move||{
            let(path,_)=request(&mut stream);
            if path.contains("/requests "){entered.send(()).unwrap();let(lock,condition)=&*gate;let mut released=lock.lock().unwrap();while !*released{released=condition.wait(released).unwrap();}respond(&mut stream,502,r#"{"attempt_id":"attempt","provider_status":404,"code":"provider_rejected","retryable":false,"retry_after_seconds":0}"#);}
            else {assert!(path.contains("/renew "));respond(&mut stream,200,r#"{"lease_until":"2099-01-01T00:00:00Z","lease_remaining_ms":120000}"#);}
        }));
        }
        for handler in handlers {
            handler.join().unwrap();
        }
    });
    let client = Client::new(
        &format!("http://{address}"),
        "workspace",
        "collection",
        "connection",
        "nebula-grant",
    )
    .unwrap();
    let mut tasks = Vec::new();
    for _ in 0..4 {
        let client = client.clone();
        tasks.push(tokio::spawn(async move {
            client.provider(&run(), "attempt", "read", json!({})).await
        }));
    }
    tokio::task::spawn_blocking(move || {
        for _ in 0..4 {
            received.recv_timeout(Duration::from_secs(5)).unwrap();
        }
    })
    .await
    .unwrap();
    let extra = client.provider(&run(), "extra", "read", json!({})).await;
    let renewal = client.renew(&run()).await;
    *release.0.lock().unwrap() = true;
    release.1.notify_all();
    for task in tasks {
        assert!(task.await.unwrap().is_err());
    }
    server.join().unwrap();
    assert!(matches!(extra, Err(ClientError::Capacity)));
    assert!(renewal.is_ok());
}

#[test]
fn credentials_cannot_go_to_arbitrary_cleartext_or_url_userinfo() {
    for endpoint in [
        "http://provider.example",
        "https://user@provider.example",
        "https://provider.example/?token=x",
        "https://provider.example/#x",
    ] {
        assert!(Client::new(endpoint, "w", "c", "connection", "token").is_err());
    }
}

struct ReleaseGate(Arc<(Mutex<bool>, Condvar)>);
impl Drop for ReleaseGate {
    fn drop(&mut self) {
        *self.0.0.lock().unwrap() = true;
        self.0.1.notify_all();
    }
}

#[tokio::test]
async fn non_json_overload_keeps_retry_after() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        let (mut stream, _) = accept(&listener);
        let _ = request(&mut stream);
        write!(stream,"HTTP/1.1 503 Unavailable\r\nContent-Length: 9\r\nRetry-After: 30\r\nConnection: close\r\n\r\noverload!").unwrap();
    });
    let client = Client::new(
        &format!("http://{address}"),
        "workspace",
        "collection",
        "connection",
        "nebula-grant",
    )
    .unwrap();
    let result = client.renew(&run()).await;
    assert!(
        matches!(result,Err(ClientError::Rejected{status:503,retry_after:Some(d)}) if d==Duration::from_secs(30))
    );
    server.join().unwrap();
}

#[tokio::test]
async fn maximum_cursors_and_receipts_survive_http_recovery() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let batch_id = "🦀".repeat(256);
    let server_batch = batch_id.clone();
    let server = thread::spawn(move || {
        let (mut stream, _) = accept(&listener);
        let (path, _) = request(&mut stream);
        assert!(path.contains("/runs "));
        let mut run = run();
        run.cursor = json!({"x":"x".repeat(65528)});
        assert_eq!(serde_json::to_vec(&run.cursor).unwrap().len(), 65536);
        respond(&mut stream, 200, &serde_json::to_string(&run).unwrap());
        let (mut stream, _) = accept(&listener);
        let (path, body) = request(&mut stream);
        assert!(path.contains("/receipts "));
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&body).unwrap()["batch_id"],
            server_batch
        );
        respond(&mut stream,200,&json!({"batch_id":server_batch,"fence":1,"sequence":0,"sha256":"a".repeat(64),"changes":vec!["🦀".repeat(256);100]}).to_string());
    });
    let client = Client::new(
        &format!("http://{address}"),
        "workspace",
        "collection",
        "connection",
        "nebula-grant",
    )
    .unwrap();
    let run = client.begin("run", "scan", true).await.unwrap();
    assert_eq!(serde_json::to_vec(&run.cursor).unwrap().len(), 65536);
    let receipt = client.receipt(&run, &batch_id).await.unwrap();
    assert_eq!(receipt.changes.len(), 100);
    assert_eq!(receipt.batch_id, batch_id);
    server.join().unwrap();
}

#[tokio::test]
async fn begin_rejects_missing_or_incompatible_advertised_protocol_limits() {
    for limits in [
        None,
        Some(json!({"max_batch_events":99,"max_batch_bytes":1048576,"max_cursor_bytes":65536})),
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let (mut stream, _) = accept(&listener);
            request(&mut stream);
            let mut body = serde_json::to_value(run()).unwrap();
            if let Some(limits) = limits {
                body["limits"] = limits;
            } else {
                body.as_object_mut().unwrap().remove("limits");
            }
            respond(&mut stream, 200, &body.to_string());
        });
        let client = Client::new(
            &format!("http://{address}"),
            "workspace",
            "collection",
            "connection",
            "nebula-grant",
        )
        .unwrap();
        assert!(matches!(
            client.begin("run", "scan", true).await,
            Err(ClientError::Response)
        ));
        server.join().unwrap();
    }
}

#[test]
fn advertised_limits_match_the_canonical_schema_and_batch_event_bound() {
    let mut schema: serde_json::Value =
        serde_json::from_str(include_str!("../schema/protocol-v1.json")).unwrap();
    let limits = nebula_connector_sdk::client::ProtocolLimits::V1;
    assert_eq!(
        schema["definitions"]["Batch"]["properties"]["events"]["maxItems"],
        limits.max_batch_events
    );
    schema["$ref"] = json!("#/definitions/ProtocolLimits");
    let validator = jsonschema::validator_for(&schema).unwrap();
    validator
        .validate(&serde_json::to_value(limits).unwrap())
        .unwrap();
}
