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

const DOCX: &str = "application/vnd.openxmlformats-officedocument.wordprocessingml.document";
const XLSX: &str = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";

fn eligible(max_bytes: u64) -> Value {
    json!({"eligible":true,"max_bytes":max_bytes,"reason":null})
}

#[tokio::test]
async fn onedrive_reads_text_transfers_office_files_and_leaves_subfolders_across_pages() {
    let server = Server::start_as(
        "onedrive",
        ACCOUNT,
        vec![
            me(),
            json!({"value":[item("N1","notes.md","application/octet-stream",20),{"id":"S1","name":"Sub","folder":{"childCount":1}}],
                "@odata.nextLink":"https://graph.microsoft.com/v1.0/me/drive/items/F1/children?$top=2&$skiptoken=p%3D2"}),
            json!("# Notes\nShip Friday"),
            json!({"value":[item("W1","plan.docx",DOCX,4096)]}),
        ],
        json!({"containers":["F1"]}),
    )
    .await;
    server
        .eligibility_for("plan.docx", eligible(64 << 20))
        .await;
    server
        .transfer_for("plan.docx", json!({"byte_length":4100}))
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
        ],
        "an original never passes through a provider request"
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
    let transfers = server.transfer_requests().await;
    assert_eq!(transfers.len(), 1);
    assert_eq!(
        (
            &transfers[0]["operation"],
            &transfers[0]["arguments"],
            &transfers[0]["object_id"],
            &transfers[0]["filename"],
            &transfers[0]["content_type"],
            &transfers[0]["max_response_bytes"],
        ),
        (
            &json!("items.original"),
            &json!({"file":"W1"}),
            &json!("W1"),
            &json!("plan.docx"),
            &json!(DOCX),
            &json!(64 << 20),
        )
    );
    assert_eq!(transfers[0]["revision"], word[1]["source"]["revision"]);
    assert_eq!(
        (word[0]["outcome"].as_str(), word[0]["sha256"].as_str()),
        (Some("fetched"), Some("a".repeat(64).as_str()))
    );
    assert_eq!(word[1]["type"], "upsert");
    assert_eq!(
        word[1]["content"],
        json!({"kind":"file","upload_id":"upload-W1","sha256":"a".repeat(64),"byte_length":4100,
            "filename":"plan.docx","media_type":DOCX,"representation":"original"})
    );
    assert_eq!(word[1]["metadata"]["filename"], "plan.docx");
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
async fn sharepoint_reads_the_files_in_a_librarys_folders_deepest_first() {
    let folder = |id: &str, name: &str| json!({"id":id,"name":name,"folder":{"childCount":1}});
    let text = |id: &str, name: &str| item(id, name, "text/plain", 4);
    let server = Server::start_as(
        "sharepoint",
        ACCOUNT,
        vec![
            me(),
            json!({"value":[folder("D1","Reports"),text("T1","top.txt")],
                "@odata.nextLink":"https://graph.microsoft.com/v1.0/drives/b!lib1/root/children?$top=2&$skiptoken=top2"}),
            json!("top"),
            json!({"value":[folder("D2","2026"),text("T2","summary.txt")],
                "@odata.nextLink":"https://graph.microsoft.com/v1.0/drives/b!lib1/items/D1/children?$top=2&$skiptoken=reports2"}),
            json!("summary"),
            json!({"value":[text("T3","q1.txt")]}),
            json!("q1"),
            json!({"value":[text("T4","notes.txt")]}),
            json!("notes"),
            json!({"value":[text("T5","last.txt")]}),
            json!("last"),
        ],
        json!({"containers":["b!lib1"]}),
    )
    .await;

    assert_eq!(server.run(&SharePoint).await.unwrap().state, "sealed");
    let listings = server
        .operations()
        .await
        .into_iter()
        .filter(|(operation, _)| operation.ends_with(".children"))
        .map(|(operation, arguments)| {
            (
                operation,
                arguments["folder"].as_str().map(str::to_owned),
                arguments["skiptoken"].as_str().map(str::to_owned),
            )
        })
        .collect::<Vec<_>>();
    let listing = |operation: &str, folder: Option<&str>, page: Option<&str>| {
        (
            operation.to_owned(),
            folder.map(str::to_owned),
            page.map(str::to_owned),
        )
    };
    assert_eq!(
        listings,
        [
            listing("drives.root.children", None, None),
            listing("drives.items.children", Some("D1"), None),
            listing("drives.items.children", Some("D2"), None),
            listing("drives.items.children", Some("D1"), Some("reports2")),
            listing("drives.root.children", None, Some("top2")),
        ]
    );
    let events = server.events().await;
    for (id, path) in [
        ("T1", "b!lib1/top.txt"),
        ("T2", "b!lib1/Reports/summary.txt"),
        ("T3", "b!lib1/Reports/2026/q1.txt"),
        ("T4", "b!lib1/Reports/notes.txt"),
        ("T5", "b!lib1/last.txt"),
    ] {
        let file = of_object(&events, id);
        assert_eq!(file[0]["metadata"]["path"], path);
        assert_eq!(file[0]["metadata"]["container_id"], "b!lib1");
        assert_eq!(file[1]["type"], "upsert", "{id}");
    }
    assert!(of_object(&events, "D1").is_empty() && of_object(&events, "D2").is_empty());
}

#[tokio::test]
async fn a_library_whose_open_folders_outgrow_the_checkpoint_fails_the_scan() {
    let more = format!(
        "https://graph.microsoft.com/v1.0/drives/b!lib1/root/children?$skiptoken={}",
        "p".repeat(3900)
    );
    let mut replies = vec![me()];
    for depth in 0..20 {
        replies.push(json!({"value":[
            {"id":format!("A{depth}"),"name":"a","folder":{"childCount":1}},
            {"id":format!("B{depth}"),"name":"b","folder":{"childCount":1}}],
            "@odata.nextLink":more}));
    }
    let server = Server::start_as(
        "sharepoint",
        ACCOUNT,
        replies,
        json!({"containers":["b!lib1"]}),
    )
    .await;

    assert!(server.run(&SharePoint).await.is_err());
    let listed = server.operations().await.len();
    assert!(
        (10..20).contains(&listed),
        "the scan stops once its position passes the checkpoint bound, after {listed} requests"
    );
    assert!(
        server
            .events()
            .await
            .iter()
            .all(|event| event["type"] != "snapshot_complete"),
        "an unfinished library must not complete the inventory"
    );
}

#[tokio::test]
async fn sharepoint_transfers_what_the_engine_accepts_and_explains_what_it_leaves() {
    let server = Server::start_as(
        "sharepoint",
        ACCOUNT,
        vec![
            me(),
            json!({"value":[item("P1","deck.pdf","application/pdf",5 << 20),item("L1","log.txt","text/plain",311_000)],
                "@odata.nextLink":"https://graph.microsoft.com/v1.0/drives/b!lib1/root/children?$top=2&$skiptoken=2"}),
            json!({"value":[item("X1","budget.xlsx",XLSX,9000),item("B1","scan.pdf","application/pdf",9 << 30)],
                "@odata.nextLink":"https://graph.microsoft.com/v1.0/drives/b!lib1/root/children?$top=2&$skiptoken=3"}),
            json!({"value":[item("G1","grown.pdf","application/pdf",1000),item("D1","denied.docx",DOCX,1000)],
                "@odata.nextLink":"https://graph.microsoft.com/v1.0/drives/b!lib1/root/children?$top=2&$skiptoken=4"}),
            json!({"value":[item("O1","old-service.docx",DOCX,1000)]}),
        ],
        json!({"containers":["b!lib1"]}),
    )
    .await;
    for name in ["deck.pdf", "log.txt", "grown.pdf"] {
        server.eligibility_for(name, eligible(2_500_000_000)).await;
    }
    server
        .eligibility_for(
            "budget.xlsx",
            json!({"eligible":false,"max_bytes":1,"reason":"table_import_required"}),
        )
        .await;
    server
        .eligibility_for(
            "scan.pdf",
            json!({"eligible":false,"max_bytes":2_500_000_000u64,"reason":"too_large"}),
        )
        .await;
    server
        .transfer_for("log.txt", json!({"byte_length":311_000}))
        .await;
    server
        .transfer_for(
            "grown.pdf",
            json!({"provider_status":0,"code":"response_limit"}),
        )
        .await;
    server
        .transfer_for(
            "denied.docx",
            json!({"provider_status":403,"code":"provider_rejected"}),
        )
        .await;
    server
        .transfer_for("old-service.docx", json!({"status":404}))
        .await;

    assert_eq!(server.run(&SharePoint).await.unwrap().state, "sealed");
    let events = server.events().await;
    let outcome = |id: &str| {
        let file = of_object(&events, id);
        (
            file[0]["outcome"].as_str().unwrap().to_owned(),
            file[0]["reason"].as_str().unwrap().to_owned(),
            file[1]["type"].as_str().unwrap().to_owned(),
        )
    };
    let expect = |outcome: &str, reason: &str, change: &str| {
        (outcome.to_owned(), reason.to_owned(), change.to_owned())
    };
    assert_eq!(outcome("P1"), expect("fetched", "in_scope", "upsert"));
    assert_eq!(outcome("L1"), expect("fetched", "in_scope", "upsert"));
    assert_eq!(
        outcome("X1"),
        expect("skipped", "table_import_required", "presence")
    );
    assert_eq!(outcome("B1"), expect("skipped", "too_large", "presence"));
    assert_eq!(outcome("G1"), expect("skipped", "too_large", "presence"));
    assert_eq!(outcome("D1"), expect("failed", "access_denied", "presence"));
    assert_eq!(outcome("O1"), expect("skipped", "unsupported", "presence"));

    let log = of_object(&events, "L1");
    assert_eq!(
        (
            &log[1]["content"]["media_type"],
            &log[1]["content"]["byte_length"]
        ),
        (&json!("text/plain"), &json!(311_000)),
        "a text file too large to read inline is stored as it is"
    );
    let transfers = server.transfer_requests().await;
    assert_eq!(
        transfers
            .iter()
            .map(|transfer| transfer["filename"].as_str().unwrap())
            .collect::<Vec<_>>(),
        [
            "deck.pdf",
            "log.txt",
            "grown.pdf",
            "denied.docx",
            "old-service.docx"
        ],
        "a file the engine refuses is never downloaded"
    );
    assert_eq!(transfers[0]["operation"], "drives.items.original");
    assert_eq!(
        transfers[0]["arguments"],
        json!({"file":"P1","drive":"b!lib1"})
    );
    assert!(
        server
            .operations()
            .await
            .iter()
            .all(|(operation, _)| operation != "drives.items.content"),
        "no original is read as text"
    );
}

#[tokio::test]
async fn a_file_keeps_one_revision_per_representation_until_its_content_changes() {
    let revision_of = |size: u64, content_tag: &'static str| async move {
        let mut file = item("L1", "log.txt", "text/plain", size);
        file["cTag"] = content_tag.into();
        let mut replies = vec![me(), json!({"value":[file]})];
        if size <= 192 << 10 {
            replies.push(json!("text"));
        }
        let server = Server::start_as(
            "sharepoint",
            ACCOUNT,
            replies,
            json!({"containers":["b!lib1"]}),
        )
        .await;
        server.eligibility_for("log.txt", eligible(1 << 30)).await;
        server.run(&SharePoint).await.unwrap();
        let events = server.events().await;
        of_object(&events, "L1")[0]["source"]["revision"]
            .as_str()
            .unwrap()
            .to_owned()
    };
    let inline = revision_of(100, "c:1").await;
    let stored = revision_of(300_000, "c:1").await;
    assert_eq!(revision_of(300_000, "c:1").await, stored);
    assert_ne!(revision_of(300_000, "c:2").await, stored);
    assert_ne!(
        inline, stored,
        "text read inline and the stored original are different representations"
    );
}

/// Runs OneDrive over one folder that holds the files named, each a PDF the
/// engine takes, and returns the transfer requests and each file's outcome,
/// reason and change.
async fn transferred(answers: &[(&str, Value)]) -> (Vec<Value>, Vec<(String, String, String)>) {
    let files = answers
        .iter()
        .map(|(name, _)| item(name, name, "application/pdf", 4096))
        .collect::<Vec<_>>();
    let server = Server::start_as(
        "onedrive",
        ACCOUNT,
        vec![me(), json!({"value":files})],
        json!({"containers":["F1"]}),
    )
    .await;
    for (name, answer) in answers {
        server.eligibility_for(name, eligible(64 << 20)).await;
        server.transfer_for(name, answer.clone()).await;
    }
    assert_eq!(server.run(&OneDrive).await.unwrap().state, "sealed");
    let events = server.events().await;
    let outcomes = answers
        .iter()
        .map(|(name, _)| {
            let file = of_object(&events, name);
            let text = |value: &Value| value.as_str().unwrap().to_owned();
            (
                text(&file[0]["outcome"]),
                text(&file[0]["reason"]),
                text(&file[1]["type"]),
            )
        })
        .collect();
    (server.transfer_requests().await, outcomes)
}

#[tokio::test]
async fn an_upload_the_engine_has_not_verified_is_named_instead_of_read_again() {
    let (requests, outcomes) = transferred(&[(
        "slow.pdf",
        json!([{"unverified":"upload-slow"},{"unverified":"upload-slow"},{"byte_length":4096}]),
    )])
    .await;
    assert_eq!(
        requests
            .iter()
            .map(|request| request["upload_id"].as_str())
            .collect::<Vec<_>>(),
        [None, Some("upload-slow"), Some("upload-slow")]
    );
    assert_ne!(requests[0]["id"], requests[1]["id"]);
    assert_eq!(
        outcomes,
        [("fetched".into(), "in_scope".into(), "upsert".into())]
    );
}

#[tokio::test]
async fn a_busy_service_is_asked_again_and_a_failing_one_three_times() {
    let busy = json!({"status":429,"retry_after":0});
    let (requests, outcomes) = transferred(&[
        (
            "queued.pdf",
            json!([busy, busy, busy, {"byte_length":4096}]),
        ),
        ("unlucky.pdf", json!({"status":503,"retry_after":0})),
    ])
    .await;
    let asked = |name: &str| {
        requests
            .iter()
            .filter(|request| request["filename"] == name)
            .count()
    };
    assert_eq!((asked("queued.pdf"), asked("unlucky.pdf")), (4, 3));
    assert_eq!(
        outcomes,
        [
            ("fetched".into(), "in_scope".into(), "upsert".into()),
            ("failed".into(), "fetch_failed".into(), "presence".into()),
        ]
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
