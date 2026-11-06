mod common;

use common::{Server, of_object};
use nebula_connectors::{
    microsoft_files::{OneDrive, SharePoint},
    outlook::Outlook,
    teams::Teams,
};
use serde_json::{Value, json};

const ACCOUNT: &str = "48d31887-5fad-4d73-a9f5-3c356e68a038";

fn me() -> Value {
    json!({"id":ACCOUNT})
}

fn item(id: &str, name: &str, mime_type: &str, size: u64) -> Value {
    json!({"id":id,"name":name,"file":{"mimeType":mime_type},"size":size,"cTag":format!("c:{id},1"),
        "createdDateTime":"2026-10-01T09:00:00Z","lastModifiedDateTime":"2026-10-27T18:46:26Z",
        "webUrl":format!("https://contoso.sharepoint.com/{id}")})
}

#[tokio::test]
async fn onedrive_reads_text_files_skips_office_files_and_subfolders_across_pages() {
    let server = Server::start_as(
        "onedrive",
        ACCOUNT,
        vec![
            me(),
            json!({"value":[item("N1","notes.md","application/octet-stream",20),{"id":"S1","name":"Sub","folder":{"childCount":1}}],
                "@odata.nextLink":"https://graph.microsoft.com/v1.0/me/drive/items/F1/children?$top=2&$skiptoken=p%3D2"}),
            json!("# Notes\nShip Friday"),
            json!({"value":[item("W1","plan.docx","application/vnd.openxmlformats-officedocument.wordprocessingml.document",4096)]}),
        ],
        json!({"containers":["F1"]}),
    )
    .await;

    assert_eq!(server.run(&OneDrive).await.unwrap().state, "sealed");
    let operations = server.operations().await;
    assert_eq!(
        operations
            .iter()
            .map(|(op, _)| op.as_str())
            .collect::<Vec<_>>(),
        [
            "me.get",
            "items.children",
            "items.content",
            "items.children"
        ]
    );
    assert_eq!(operations[2].1, json!({"file":"N1"}));
    assert_eq!(operations[3].1["skiptoken"], "p=2");
    let events = server.events().await;
    let notes = of_object(&events, "N1");
    assert_eq!(notes[1]["content"]["media_type"], "text/markdown");
    assert_eq!(notes[1]["content"]["text"], "# Notes\nShip Friday");
    assert_eq!(notes[1]["source"]["authority"], ACCOUNT);
    assert!(
        of_object(&events, "S1").is_empty(),
        "subfolders are selected on their own"
    );
    let word = of_object(&events, "W1");
    assert_eq!(
        (word[0]["reason"].as_str(), word[1]["type"].as_str()),
        (Some("unsupported"), Some("presence"))
    );
}

#[tokio::test]
async fn sharepoint_reads_a_library_and_names_the_library_when_downloading() {
    let server = Server::start_as(
        "sharepoint",
        ACCOUNT,
        vec![
            me(),
            json!({"value":[item("T1","faq.txt","text/plain",12)]}),
            json!("Q: Where? A: Here"),
        ],
        json!({"containers":["b!lib1"]}),
    )
    .await;

    assert_eq!(server.run(&SharePoint).await.unwrap().state, "sealed");
    let operations = server.operations().await;
    assert_eq!(operations[1].0, "drives.root.children");
    assert_eq!(operations[1].1["drive"], "b!lib1");
    assert_eq!(operations[2].1, json!({"file":"T1","drive":"b!lib1"}));
    let events = server.events().await;
    assert_eq!(
        of_object(&events, "T1")[1]["content"]["text"],
        "Q: Where? A: Here"
    );
}

#[tokio::test]
async fn a_signed_in_account_other_than_the_connection_stops_the_run() {
    let server = Server::start_as(
        "sharepoint",
        ACCOUNT,
        vec![json!({"id":"00000000-0000-0000-0000-000000000000"})],
        json!({"containers":["b!lib1"]}),
    )
    .await;

    assert!(server.run(&SharePoint).await.is_err());
    assert_eq!(server.operations().await.len(), 1);
}

#[tokio::test]
async fn outlook_reads_each_message_as_text_and_pages_by_skip() {
    let message = |id: &str, body: &str| {
        json!({"id":id,"subject":"Q3 plan","from":{"emailAddress":{"name":"Ada","address":"ada@contoso.com"}},
            "toRecipients":[{"emailAddress":{"address":"team@contoso.com"}}],"receivedDateTime":"2026-10-27T18:46:26Z",
            "lastModifiedDateTime":"2026-10-27T18:46:26Z","changeKey":format!("ck-{id}"),"body":{"contentType":"text","content":body},
            "webLink":format!("https://outlook.office365.com/owa/?ItemID={id}")})
    };
    let server = Server::start_as(
        "outlook",
        ACCOUNT,
        vec![
            me(),
            json!({"value":[{"id":"AAMk1="}],"@odata.nextLink":"https://graph.microsoft.com/v1.0/me/mailFolders/inbox/messages?%24select=id&%24top=4&%24skip=4"}),
            message("AAMk1=", "Revenue up 4%"),
            json!({"value":[{"id":"AAMk2="}]}),
            json!({"provider_status":404,"code":"provider_rejected"}),
        ],
        json!({"containers":["inbox"]}),
    )
    .await;

    assert_eq!(server.run(&Outlook).await.unwrap().state, "sealed");
    let operations = server.operations().await;
    assert_eq!(
        operations[3].1,
        json!({"folder":"inbox","select":"id","top":"4","skip":"4"})
    );
    let events = server.events().await;
    assert_eq!(
        of_object(&events, "AAMk1=")[1]["content"]["text"],
        "Subject: Q3 plan\nFrom: Ada <ada@contoso.com>\nTo: team@contoso.com\nDate: 2026-10-27T18:46:26Z\n\nRevenue up 4%"
    );
    let missing = of_object(&events, "AAMk2=");
    assert_eq!(
        (
            missing[0]["outcome"].as_str(),
            missing[0]["reason"].as_str()
        ),
        (Some("failed"), Some("unavailable"))
    );
}

#[tokio::test]
async fn teams_reads_each_message_then_its_replies_and_drops_deleted_ones() {
    let message = |id: &str, body: &str, deleted: bool| {
        json!({"id":id,"messageType":"message","createdDateTime":"2026-10-27T18:46:26Z","lastModifiedDateTime":null,
            "deletedDateTime":if deleted { json!("2026-10-28T00:00:00Z") } else { Value::Null },
            "body":{"contentType":"html","content":body},"from":{"user":{"displayName":"Ada"}},
            "webUrl":format!("https://teams.microsoft.com/l/message/{id}")})
    };
    let container = "T1~19:general@thread.tacv2";
    let server = Server::start_as(
        "teams",
        ACCOUNT,
        vec![
            me(),
            json!({"value":[message("100","<p>Launch Friday</p>",false)],"@odata.nextLink":"https://graph.microsoft.com/v1.0/teams/T1/channels/19:general@thread.tacv2/messages?$top=1&$skiptoken=m2"}),
            json!({"value":[message("101","<p>Agreed</p>",false),message("102","<p>gone</p>",true)]}),
            json!({"value":[message("200","<p>removed</p>",true)]}),
        ],
        json!({"containers":[container]}),
    )
    .await;

    assert_eq!(server.run(&Teams).await.unwrap().state, "sealed");
    let operations = server.operations().await;
    assert_eq!(
        operations
            .iter()
            .map(|(op, _)| op.as_str())
            .collect::<Vec<_>>(),
        ["me.get", "messages.list", "replies.list", "messages.list"]
    );
    assert_eq!(
        operations[1].1,
        json!({"team":"T1","channel":"19:general@thread.tacv2","top":"1"})
    );
    assert_eq!(operations[2].1["message"], "100");
    assert_eq!(operations[3].1["skiptoken"], "m2");
    let events = server.events().await;
    let root = of_object(&events, &format!("{container}/100"));
    assert_eq!(
        root[1]["content"],
        json!({"kind":"text","text":"<p>Launch Friday</p>","media_type":"text/html","representation_version":"teams-message-v1"})
    );
    assert_eq!(
        of_object(&events, &format!("{container}/101"))[1]["type"],
        "upsert"
    );
    assert!(of_object(&events, &format!("{container}/102")).is_empty());
    assert!(of_object(&events, &format!("{container}/200")).is_empty());
    assert!(events.iter().any(|e| e["type"] == "snapshot_complete"));
}
