mod common;

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use common::{Server, of_object};
use nebula_connectors::gmail::Gmail;
use serde_json::{Value, json};

fn profile() -> Value {
    json!({"emailAddress":"ada@example.com","messagesTotal":3})
}

fn part(mime_type: &str, text: &str) -> Value {
    json!({"mimeType":mime_type,"filename":"","body":{"size":text.len(),"data":URL_SAFE_NO_PAD.encode(text)}})
}

fn message(id: &str, payload: Value) -> Value {
    json!({"id":id,"threadId":"t1","labelIds":["INBOX"],"internalDate":"1793126786034",
        "sizeEstimate":2048,"payload":payload})
}

fn headers() -> Value {
    json!([{"name":"Subject","value":"Q3 plan"},{"name":"From","value":"Ada <ada@example.com>"},
        {"name":"To","value":"team@example.com"},{"name":"Date","value":"Tue, 27 Oct 2026 18:46:26 +0000"}])
}

#[tokio::test]
async fn label_inventory_reads_plain_bodies_falls_back_to_html_and_skips_attachments() {
    let mixed = json!({"mimeType":"multipart/mixed","headers":headers(),"parts":[
        {"mimeType":"text/plain","filename":"notes.txt","body":{"size":15,"data":URL_SAFE_NO_PAD.encode("attachment text")}},
        {"mimeType":"multipart/alternative","parts":[part("text/plain","Revenue up 4%"),part("text/html","<p>Revenue up 4%</p>")]}]});
    let html_only = json!({"mimeType":"text/html","headers":headers(),"filename":"","body":{"size":12,"data":URL_SAFE_NO_PAD.encode("<b>Hello</b>")}});
    let empty =
        json!({"mimeType":"text/plain","headers":headers(),"filename":"","body":{"size":0}});
    let server = Server::start(
        "gmail",
        vec![
            profile(),
            json!({"messages":[{"id":"m1","threadId":"t1"},{"id":"m2","threadId":"t2"},{"id":"m3","threadId":"t3"}],"resultSizeEstimate":3}),
            message("m1", mixed),
            message("m2", html_only),
            message("m3", empty),
        ],
        json!({"containers":["INBOX"]}),
    )
    .await;

    assert_eq!(server.run(&Gmail).await.unwrap().state, "sealed");
    let operations = server.operations().await;
    assert_eq!(
        operations
            .iter()
            .map(|(op, _)| op.as_str())
            .collect::<Vec<_>>(),
        [
            "users.getProfile",
            "users.messages.list",
            "users.messages.get",
            "users.messages.get",
            "users.messages.get"
        ]
    );
    assert_eq!(operations[1].1, json!({"label":"INBOX","maxResults":"4"}));
    assert_eq!(operations[2].1, json!({"message":"m1","format":"full"}));
    let events = server.events().await;
    let plain = of_object(&events, "m1");
    assert_eq!(plain[0]["outcome"], "fetched");
    assert_eq!(plain[0]["metadata"]["created_at_ms"], 1_793_126_786_034_i64);
    assert_eq!(
        plain[1]["content"],
        json!({"kind":"text","media_type":"text/plain","representation_version":"gmail-message-v1",
            "text":"Subject: Q3 plan\nFrom: Ada <ada@example.com>\nTo: team@example.com\nDate: Tue, 27 Oct 2026 18:46:26 +0000\n\nRevenue up 4%"})
    );
    assert_eq!(plain[1]["metadata"]["subject"], "Q3 plan");
    let html = of_object(&events, "m2");
    assert_eq!(html[1]["content"]["media_type"], "text/html");
    assert_eq!(html[1]["content"]["text"], "<b>Hello</b>");
    let empty = of_object(&events, "m3");
    assert_eq!(
        (empty[0]["outcome"].as_str(), empty[0]["reason"].as_str()),
        (Some("skipped"), Some("empty"))
    );
    assert_eq!(empty[1]["type"], "presence");
}

#[tokio::test]
async fn a_message_that_cannot_be_read_fails_alone_and_pages_continue_in_the_label() {
    let server = Server::start(
        "gmail",
        vec![
            profile(),
            json!({"messages":[{"id":"m1","threadId":"t1"}],"nextPageToken":"p2"}),
            json!({"provider_status":404,"code":"provider_rejected"}),
            json!({"messages":[{"id":"m2","threadId":"t2"}]}),
            message("m2", json!({"mimeType":"text/plain","headers":headers(),"filename":"","body":{"size":5,"data":URL_SAFE_NO_PAD.encode("Hello")}})),
            json!({"resultSizeEstimate":0}),
        ],
        json!({"containers":["INBOX","Label_1"]}),
    )
    .await;

    assert_eq!(server.run(&Gmail).await.unwrap().state, "sealed");
    let operations = server.operations().await;
    assert_eq!(
        operations[3].1,
        json!({"label":"INBOX","maxResults":"4","pageToken":"p2"})
    );
    assert_eq!(operations[5].1, json!({"label":"Label_1","maxResults":"4"}));
    let events = server.events().await;
    let missing = of_object(&events, "m1");
    assert_eq!(
        (
            missing[0]["outcome"].as_str(),
            missing[0]["reason"].as_str()
        ),
        (Some("failed"), Some("unavailable"))
    );
    assert_eq!(of_object(&events, "m2")[1]["type"], "upsert");
    assert!(events.iter().any(|e| e["type"] == "snapshot_complete"));
}
