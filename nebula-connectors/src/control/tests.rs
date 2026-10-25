use std::sync::{Condvar, Mutex};

use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const TOKEN: &str = "test-only-workload-key-0123456789012345";

struct Gate(Arc<(Mutex<bool>, Condvar)>);
impl Drop for Gate {
    fn drop(&mut self) {
        *self.0.0.lock().unwrap() = true;
        self.0.1.notify_all();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(
    clippy::disallowed_methods,
    reason = "Local HTTP proof with bounded client deadline; provider verification is deliberately blocked"
)]
async fn verifier_deadlines_keep_ownership_and_leave_other_provider_workers_available() {
    let gate = Gate(Arc::new((Mutex::new(false), Condvar::new())));
    let worker_gate = gate.0.clone();
    let (entered, mut arrivals) = tokio::sync::mpsc::channel(2);
    let processor = Arc::new(move |provider, bytes: &[u8]| {
        if provider == Provider::Slack {
            entered.try_send(()).unwrap();
            let guard = worker_gate.0.lock().unwrap();
            drop(worker_gate.1.wait_while(guard, |open| !*open).unwrap());
        }
        process(provider, bytes)
    });
    let runtime = Runtime::with_processor(
        Credentials {
            tokens: vec![TOKEN.to_owned()],
        },
        processor,
    )
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(serve(listener, runtime.clone(), async {
        let _ = stopped.await;
    }));
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(4))
        .build()
        .unwrap();
    let slack = format!("http://{address}/v1/providers/slack/webhook/verify");
    let mut pending = Vec::new();
    for _ in 0..8 {
        let request = client
            .post(&slack)
            .bearer_auth(TOKEN)
            .json(&serde_json::json!({}));
        pending.push(tokio::spawn(async move { request.send().await.unwrap() }));
    }
    for _ in 0..2 {
        tokio::time::timeout(Duration::from_secs(1), arrivals.recv())
            .await
            .unwrap()
            .unwrap();
    }
    let response = client
        .post(format!(
            "http://{address}/v1/providers/notion/webhook/verify"
        ))
        .bearer_auth(TOKEN)
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 400);
    for request in pending {
        assert_eq!(request.await.unwrap().status(), 503);
    }
    assert!(
        runtime.0.callbacks[&Provider::Slack].acquire().is_none(),
        "timed-out work must retain its permit"
    );
    let response = client
        .post(&slack)
        .bearer_auth(TOKEN)
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 503);
    drop(gate);
    stop.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(4), server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn accepted_socket_limit_covers_incomplete_headers_and_drain_closes_them() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let runtime = Runtime::new(Credentials {
        tokens: vec![TOKEN.to_owned()],
    })
    .unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(serve(listener, runtime, async {
        let _ = stopped.await;
    }));
    let mut connections = Vec::new();
    for _ in 0..128 {
        let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
        stream
            .write_all(b"GET /healthz HTTP/1.1\r\nHost: local\r\n")
            .await
            .unwrap();
        connections.push(stream);
    }
    let mut last = tokio::net::TcpStream::connect(address).await.unwrap();
    last.write_all(b"GET /healthz HTTP/1.1\r\nHost: local\r\n\r\n")
        .await
        .unwrap();
    let mut buffer = [0; 256];
    assert!(
        tokio::time::timeout(Duration::from_millis(50), last.read(&mut buffer))
            .await
            .is_err(),
        "extra socket was admitted beyond the fixed bound"
    );
    connections[0].write_all(b"\r\n").await.unwrap();
    let n = tokio::time::timeout(Duration::from_secs(1), connections[0].read(&mut buffer))
        .await
        .unwrap()
        .unwrap();
    assert!(
        std::str::from_utf8(&buffer[..n])
            .unwrap()
            .starts_with("HTTP/1.1 204")
    );
    let n = tokio::time::timeout(Duration::from_secs(1), last.read(&mut buffer))
        .await
        .unwrap()
        .unwrap();
    assert!(
        std::str::from_utf8(&buffer[..n])
            .unwrap()
            .starts_with("HTTP/1.1 204")
    );
    stop.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(4), server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    for stream in connections.iter_mut().skip(1) {
        let read = tokio::time::timeout(Duration::from_secs(1), stream.read(&mut buffer))
            .await
            .unwrap();
        assert!(
            read.is_err() || read.unwrap() == 0,
            "drained idle connection survived"
        );
    }
}
