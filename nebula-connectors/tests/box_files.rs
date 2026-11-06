mod common;

use common::{Server, of_object};
use nebula_connectors::box_files::BoxFiles;
use serde_json::{Value, json};

const SHA1: &str = "85136c79cbf9fe36bb9d05d0639c70c265c18d37";

fn file(id: &str, name: &str, size: u64) -> Value {
    json!({"type":"file","id":id,"name":name,"size":size,"sha1":SHA1,
        "created_at":"2026-10-01T02:00:00-07:00","modified_at":"2026-10-27T11:46:00-07:00"})
}

#[tokio::test]
async fn box_folders_read_text_files_and_page_by_marker() {
    let server = Server::start_as(
        "box",
        "7001",
        vec![
            json!({"type":"user","id":"7001"}),
            json!({"entries":[file("101", "Launch notes.md", 12), {"type":"folder","id":"102","name":"Archive"}],
                "next_marker":"m2"}),
            json!("# Launch\nShip Friday\n"),
            json!({"entries":[file("103", "deck.pptx", 4096), file("104", "secret.txt", 9)],"next_marker":null}),
            json!({"provider_status":403,"code":"provider_rejected"}),
        ],
        json!({"containers":["55"]}),
    )
    .await;

    assert_eq!(server.run(&BoxFiles).await.unwrap().state, "sealed");
    let operations = server.operations().await;
    assert_eq!(
        operations
            .iter()
            .map(|(op, _)| op.as_str())
            .collect::<Vec<_>>(),
        [
            "users.me",
            "folders.items",
            "files.content",
            "folders.items",
            "files.content"
        ]
    );
    let fields = "id,type,name,size,sha1,created_at,modified_at";
    assert_eq!(
        operations[1].1,
        json!({"folder":"55","fields":fields,"limit":"2","usemarker":"true"})
    );
    assert_eq!(
        operations[3].1,
        json!({"folder":"55","fields":fields,"limit":"2","usemarker":"true","marker":"m2"})
    );
    assert_eq!(operations[2].1, json!({"file":"101"}));

    let events = server.events().await;
    let notes = of_object(&events, "101");
    assert_eq!(notes[0]["metadata"]["created_at_ms"], 1_790_845_200_000_i64);
    assert_eq!(notes[1]["content"]["text"], "# Launch\nShip Friday\n");
    assert_eq!(notes[1]["content"]["media_type"], "text/markdown");
    assert_eq!(notes[0]["source"]["url"], "https://app.box.com/file/101");
    assert!(of_object(&events, "102").is_empty());
    let deck = of_object(&events, "103");
    assert_eq!(
        (&deck[0]["outcome"], &deck[0]["reason"]),
        (&json!("skipped"), &json!("unsupported"))
    );
    let secret = of_object(&events, "104");
    assert_eq!(
        (&secret[0]["outcome"], &secret[0]["reason"]),
        (&json!("failed"), &json!("access_denied"))
    );
}

#[tokio::test]
async fn box_refuses_a_token_for_another_user() {
    let server = Server::start_as(
        "box",
        "7001",
        vec![json!({"type":"user","id":"9999"})],
        json!({"containers":["55"]}),
    )
    .await;
    assert!(server.run(&BoxFiles).await.is_err());
    assert_eq!(server.operations().await.len(), 1);
}
