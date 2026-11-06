mod common;

use common::{Server, of_object};
use nebula_connectors::dropbox::Dropbox;
use serde_json::{Value, json};

const ACCOUNT: &str = "dbid:AAH4f99T0taONIb-OurWxbNQ6ywGRopQngc";

fn file(id: &str, name: &str, downloadable: bool) -> Value {
    json!({".tag":"file","id":id,"name":name,"size":5,"is_downloadable":downloadable,
        "content_hash":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        "server_modified":"2026-10-27T18:46:00Z","client_modified":"2026-10-27T18:40:00Z","rev":"a1c10ce0dd78"})
}

#[tokio::test]
async fn dropbox_folders_read_text_files_by_id_and_continue_with_the_cursor() {
    let server = Server::start_as(
        "dropbox",
        ACCOUNT,
        vec![
            json!({"account_id":ACCOUNT,"email":"ada@example.com"}),
            json!({"entries":[file("id:A1", "plan.txt", true)],"cursor":"c1","has_more":true}),
            json!("hello"),
            json!({"entries":[{".tag":"folder","id":"id:SUB","name":"Old"},file("id:B2", "locked.txt", false)],
                "cursor":"c2","has_more":false}),
        ],
        json!({"containers":["id:F1"]}),
    )
    .await;

    assert_eq!(server.run(&Dropbox).await.unwrap().state, "sealed");
    assert_eq!(
        server.operations().await,
        [
            ("users.get_current_account".to_owned(), json!({})),
            ("files.list_folder".to_owned(), json!({"folder":"id:F1"})),
            (
                "files.download".to_owned(),
                json!({"arg":"{\"path\":\"id:A1\"}"})
            ),
            (
                "files.list_folder_continue".to_owned(),
                json!({"cursor":"c1"})
            ),
        ]
    );
    let events = server.events().await;
    let plan = of_object(&events, "id:A1");
    assert_eq!(plan[0]["metadata"]["modified_at_ms"], 1_793_126_760_000_i64);
    assert_eq!(plan[1]["content"]["text"], "hello");
    assert_eq!(plan[1]["content"]["media_type"], "text/plain");
    let locked = of_object(&events, "id:B2");
    assert_eq!(
        (&locked[0]["outcome"], &locked[0]["reason"]),
        (&json!("skipped"), &json!("unsupported"))
    );
    assert!(of_object(&events, "id:SUB").is_empty());
}
