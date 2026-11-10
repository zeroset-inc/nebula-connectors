mod common;

use common::{Server, of_object};
use nebula_connector_sdk::runner;
use nebula_connectors::google_drive::GoogleDrive;
use serde_json::{Value, json};

fn about() -> Value {
    json!({"user":{"permissionId":"0123"}})
}

fn file(id: &str, name: &str, mime_type: &str, version: &str) -> Value {
    json!({"id":id,"name":name,"mimeType":mime_type,"createdTime":"2026-10-01T09:00:00.000Z",
        "modifiedTime":"2026-10-27T18:46:26.034Z","version":version,
        "webViewLink":format!("https://docs.google.com/d/{id}")})
}

fn listing(files: Vec<Value>, next: &str) -> Value {
    let mut listing = json!({"files":files,"incompleteSearch":false});
    if !next.is_empty() {
        listing["nextPageToken"] = next.into();
    }
    listing
}

#[tokio::test]
async fn selected_folder_inventory_exports_docs_reads_text_and_skips_other_files() {
    let mut notes = file("T1", "notes.md", "text/markdown", "4");
    notes["size"] = "21".into();
    let server = Server::start(
        "google_drive",
        vec![
            about(),
            listing(
                vec![
                    file("D1", "Plan", "application/vnd.google-apps.document", "12"),
                    file("P1", "scan.pdf", "application/pdf", "3"),
                ],
                "page-2",
            ),
            json!("Quarterly plan\nRevenue up 4%"),
            listing(vec![notes], ""),
            json!("# Notes\nShip Friday"),
        ],
        json!({"containers":["F1"]}),
    )
    .await;

    let completed = server.run(&GoogleDrive).await.unwrap();

    assert_eq!(completed.state, "sealed");
    let operations = server.operations().await;
    assert_eq!(
        operations
            .iter()
            .map(|(op, _)| op.as_str())
            .collect::<Vec<_>>(),
        [
            "about.get",
            "files.list",
            "files.export",
            "files.list",
            "files.download"
        ]
    );
    assert_eq!(operations[1].1["folder"], "F1");
    assert_eq!(operations[3].1["pageToken"], "page-2");
    assert_eq!(
        operations[2].1,
        json!({"file":"D1","mimeType":"text/plain"})
    );
    assert_eq!(
        operations[4].1,
        json!({"file":"T1","alt":"media","supportsAllDrives":"true"})
    );
    let events = server.events().await;
    let doc = of_object(&events, "D1");
    assert_eq!(doc[0]["type"], "item_observed");
    assert_eq!(doc[0]["outcome"], "fetched");
    assert_eq!(doc[0]["metadata"]["container_id"], "F1");
    assert_eq!(doc[0]["metadata"]["modified_at_ms"], 1_793_126_786_034_i64);
    assert_eq!(doc[1]["type"], "upsert");
    assert_eq!(
        doc[1]["content"],
        json!({"kind":"text","text":"Quarterly plan\nRevenue up 4%","media_type":"text/plain",
            "representation_version":"google-drive-text-v1"})
    );
    assert_eq!(doc[1]["source"]["authority"], "1077");
    assert_eq!(doc[1]["source"]["url"], "https://docs.google.com/d/D1");
    let pdf = of_object(&events, "P1");
    assert_eq!(
        (pdf[0]["outcome"].as_str(), pdf[0]["reason"].as_str()),
        (Some("skipped"), Some("unsupported"))
    );
    assert_eq!(pdf[1]["type"], "presence");
    let notes = of_object(&events, "T1");
    assert_eq!(notes[1]["content"]["media_type"], "text/markdown");
    assert!(events.iter().any(|e| e["type"] == "snapshot_complete"));
}

#[tokio::test]
async fn a_file_that_cannot_be_exported_fails_alone_and_the_inventory_completes() {
    let server = Server::start(
        "google_drive",
        vec![
            about(),
            listing(
                vec![
                    file("D1", "Locked", "application/vnd.google-apps.document", "2"),
                    file("D2", "Huge", "application/vnd.google-apps.document", "5"),
                ],
                "",
            ),
            json!({"provider_status":403,"code":"provider_rejected"}),
            json!({"provider_status":200,"code":"response_limit"}),
        ],
        json!({"containers":["F1"]}),
    )
    .await;

    assert_eq!(server.run(&GoogleDrive).await.unwrap().state, "sealed");
    let events = server.events().await;
    let locked = of_object(&events, "D1");
    assert_eq!(
        (locked[0]["outcome"].as_str(), locked[0]["reason"].as_str()),
        (Some("failed"), Some("access_denied"))
    );
    assert_eq!(locked[1]["type"], "presence");
    let huge = of_object(&events, "D2");
    assert_eq!(
        (huge[0]["outcome"].as_str(), huge[0]["reason"].as_str()),
        (Some("skipped"), Some("too_large"))
    );
}

#[tokio::test]
async fn a_renamed_or_moved_file_keeps_its_revision_until_its_text_changes() {
    async fn revision(name: &str, version: &str, folder: &str, text: &str) -> Value {
        let server = Server::start(
            "google_drive",
            vec![
                about(),
                listing(
                    vec![file(
                        "D1",
                        name,
                        "application/vnd.google-apps.document",
                        version,
                    )],
                    "",
                ),
                json!(text),
            ],
            json!({"containers":[folder]}),
        )
        .await;
        assert_eq!(server.run(&GoogleDrive).await.unwrap().state, "sealed");
        let events = server.events().await;
        let doc = of_object(&events, "D1");
        assert_eq!(doc[1]["type"], "upsert");
        doc[1]["source"]["revision"].clone()
    }

    let original = revision("Plan", "12", "F1", "Quarterly plan").await;
    assert_eq!(
        revision("Plan (final)", "13", "F2", "Quarterly plan").await,
        original
    );
    assert_ne!(
        revision("Plan", "14", "F1", "Quarterly plan, revised").await,
        original
    );
}

#[tokio::test]
async fn a_file_excluded_by_type_is_never_fetched() {
    let server = Server::start(
        "google_drive",
        vec![
            about(),
            listing(
                vec![file(
                    "D1",
                    "Plan",
                    "application/vnd.google-apps.document",
                    "12",
                )],
                "",
            ),
        ],
        json!({"containers":["F1"],"content_types":["text/plain"]}),
    )
    .await;

    assert_eq!(server.run(&GoogleDrive).await.unwrap().state, "sealed");
    let operations = server.operations().await;
    assert!(!operations.iter().any(|(op, _)| op == "files.export"));
    let events = server.events().await;
    let doc = of_object(&events, "D1");
    assert_eq!(doc.len(), 1);
    assert_eq!(doc[0]["selection"], "excluded");
}

#[tokio::test]
async fn losing_access_to_the_folder_fails_the_run() {
    let server = Server::start(
        "google_drive",
        vec![
            about(),
            json!({"provider_status":404,"code":"provider_rejected"}),
        ],
        json!({"containers":["F1"]}),
    )
    .await;

    assert!(matches!(
        server.run(&GoogleDrive).await,
        Err(runner::Error::Connector(
            nebula_connector_sdk::ConnectorError::Permission
        ))
    ));
}

#[tokio::test]
async fn engine_denial_skips_download_and_unknown_export_uses_the_engine_bound() {
    let mut export = file("D1", "Plan", "application/vnd.google-apps.document", "12");
    export["size"] = "9000000".into();
    let server = Server::start(
        "google_drive",
        vec![
            about(),
            listing(
                vec![file("T1", "oversize.txt", "text/plain", "1"), export],
                "",
            ),
            json!("exported text"),
        ],
        json!({"containers":["F1"]}),
    )
    .await;
    server
        .eligibility_for(
            "oversize.txt",
            json!({"eligible":false,"max_bytes":32,"reason":"too_large"}),
        )
        .await;
    server
        .eligibility_for(
            "Plan",
            json!({"eligible":true,"max_bytes":32,"reason":null}),
        )
        .await;

    server.run(&GoogleDrive).await.unwrap();

    let operations = server.operations().await;
    assert_eq!(
        operations
            .iter()
            .map(|(op, _)| op.as_str())
            .collect::<Vec<_>>(),
        ["about.get", "files.list", "files.export"]
    );
    let events = server.events().await;
    let skipped = of_object(&events, "T1");
    assert_eq!(skipped[0]["reason"], "too_large");
    assert_eq!(skipped[1]["type"], "presence");
    assert_eq!(of_object(&events, "D1")[1]["metadata"]["filename"], "Plan");
    let requests = server.eligibility_requests().await;
    assert_eq!(
        requests[1],
        json!({"fence":1,"filename":"Plan","content_type":"text/plain","size_bytes":null})
    );
    assert_eq!(
        server.provider_requests().await[2]["max_response_bytes"],
        32
    );
}
