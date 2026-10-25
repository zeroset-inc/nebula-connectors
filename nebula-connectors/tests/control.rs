use std::time::Duration;

use nebula_connectors::control::{Credentials, Runtime};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const TOKEN: &str = "local-test-workload-credential-0123456789";

struct Server {
    address: std::net::SocketAddr,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Server {
    async fn start() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let runtime = Runtime::new(Credentials {
            tokens: vec![TOKEN.to_owned()],
        })
        .unwrap();
        let task = tokio::spawn(async move {
            nebula_connectors::control::serve(listener, runtime, std::future::pending())
                .await
                .unwrap()
        });
        Self { address, task }
    }
    fn url(&self, provider: &str) -> String {
        format!(
            "http://{}/v1/providers/{provider}/webhook/verify",
            self.address
        )
    }
}

fn drive() -> Value {
    let cases: Vec<Value> =
        serde_json::from_str(include_str!("../fixtures/webhooks-v1.json")).unwrap();
    cases
        .into_iter()
        .find(|c| c["name"] == "drive hint binds channel and resource")
        .unwrap()["request"]
        .clone()
}

fn http() -> reqwest::Client {
    reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(4))
        .build()
        .unwrap()
}

#[tokio::test]
#[allow(
    clippy::disallowed_methods,
    reason = "Local HTTP component test with a fixed response fixture and four-second client deadline"
)]
async fn rpc_requires_workload_identity_and_never_echoes_verification_material() {
    let server = Server::start().await;
    let client = http();
    let url = server.url("google_drive");
    let payload = drive();
    let denied = client.post(&url).json(&payload).send().await.unwrap();
    assert_eq!(denied.status(), 401);
    let allowed = client
        .post(&url)
        .bearer_auth(TOKEN)
        .json(&payload)
        .send()
        .await
        .unwrap();
    assert_eq!(allowed.status(), 200);
    assert_eq!(allowed.headers()["cache-control"], "no-store");
    let body: Value = allowed.json().await.unwrap();
    assert_eq!(body["context"], payload["context"]);
    assert_eq!(body["outcome"]["events"][0]["kind"], "reconcile");
    assert!(!body.to_string().contains("fixture-secret"));
    assert!(body.get("verification").is_none());
    let corrupt = client
        .post(&url)
        .bearer_auth(TOKEN)
        .header("content-type", "application/json")
        .body("{\"context\":{},\"context\":{}}")
        .send()
        .await
        .unwrap();
    assert_eq!(corrupt.status(), 400);
    let oversized = client
        .post(&url)
        .bearer_auth(TOKEN)
        .header("content-type", "application/json")
        .body(" ".repeat((1 << 20) + 1))
        .send()
        .await
        .unwrap();
    assert_eq!(oversized.status(), 413);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(
    clippy::disallowed_methods,
    reason = "Local HTTP capacity test with fixed responses and four-second client deadline"
)]
async fn callback_storm_cannot_consume_another_providers_reserved_capacity() {
    let server = Server::start().await;
    let client = http();
    let mut streams = Vec::new();
    for _ in 0..8 {
        let mut stream = tokio::net::TcpStream::connect(server.address)
            .await
            .unwrap();
        stream.write_all(format!("POST /v1/providers/slack/webhook/verify HTTP/1.1\r\nHost: local\r\nAuthorization: Bearer {TOKEN}\r\nContent-Type: application/json\r\nContent-Length: 1000\r\n\r\n").as_bytes()).await.unwrap();
        streams.push(stream);
    }
    let blocked = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let response = client
                .post(server.url("slack"))
                .bearer_auth(TOKEN)
                .json(&json!({}))
                .send()
                .await
                .unwrap();
            if response.status() == 503 {
                break response;
            }
            assert_eq!(response.status(), 400);
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("Slack lane saturated");
    assert_eq!(blocked.headers()["retry-after"], "1");
    let other = client
        .post(server.url("google_drive"))
        .bearer_auth(TOKEN)
        .json(&drive())
        .send()
        .await
        .unwrap();
    assert_eq!(other.status(), 200);
    for stream in &mut streams {
        stream.write_all(&vec![b' '; 1000]).await.unwrap();
    }
    for mut stream in streams {
        let mut reply = [0; 256];
        let size = tokio::time::timeout(Duration::from_secs(1), stream.read(&mut reply))
            .await
            .unwrap()
            .unwrap();
        assert!(
            std::str::from_utf8(&reply[..size])
                .unwrap()
                .starts_with("HTTP/1.1 400")
        );
    }
}
