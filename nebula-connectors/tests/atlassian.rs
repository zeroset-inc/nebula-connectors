mod common;

use common::{Server, of_object};
use nebula_connectors::{confluence::Confluence, jira::Jira};
use serde_json::{Value, json};

const ACCOUNT: &str = "557058:f1b2c3d4";
const CLOUD: &str = "11111111-2222-3333-4444-555555555555";
const OCT_1_0900_UTC: i64 = 1_790_845_200_000;
const OCT_27_1846_UTC: i64 = 1_793_126_760_000;

fn me() -> Value {
    json!({"account_id":ACCOUNT,"email":"ada@example.com"})
}

fn confluence_page(id: &str, title: &str, body: &str) -> Value {
    json!({"id":id,"spaceId":"98304","title":title,"createdAt":"2026-10-01T09:00:00.000Z",
        "version":{"number":3,"createdAt":"2026-10-27T18:46:00.000Z"},
        "body":{"storage":{"value":body,"representation":"storage"}}})
}

#[tokio::test]
async fn confluence_pages_sync_as_stored_html_and_unreadable_pages_are_reported() {
    let space = format!("{CLOUD}~98304");
    let server = Server::start_as(
        "confluence",
        ACCOUNT,
        vec![
            me(),
            json!({"results":[{"id":"1001"},{"id":"1002"}],
                "_links":{"next":"/wiki/api/v2/spaces/98304/pages?cursor=abc%3D&limit=4"}}),
            confluence_page("1001", "Launch <plan>", "<p>Ship Friday</p>"),
            json!({"provider_status":403,"code":"provider_rejected"}),
            json!({"results":[{"id":"1003"}],"_links":{}}),
            json!({"provider_status":200,"code":"response_limit"}),
        ],
        json!({"containers":[space]}),
    )
    .await;

    assert_eq!(server.run(&Confluence).await.unwrap().state, "sealed");
    let operations = server.operations().await;
    assert_eq!(
        operations
            .iter()
            .map(|(op, _)| op.as_str())
            .collect::<Vec<_>>(),
        [
            "me.get",
            "pages.list",
            "pages.get",
            "pages.get",
            "pages.list",
            "pages.get"
        ]
    );
    assert_eq!(
        operations[1].1,
        json!({"cloud":CLOUD,"space":"98304","limit":"4","status":"current"})
    );
    assert_eq!(
        operations[4].1,
        json!({"cloud":CLOUD,"space":"98304","limit":"4","status":"current","cursor":"abc="})
    );
    assert_eq!(
        operations[2].1,
        json!({"cloud":CLOUD,"page":"1001","body-format":"storage"})
    );

    let events = server.events().await;
    let launch = of_object(&events, &format!("{CLOUD}/1001"));
    assert_eq!(launch[0]["metadata"]["created_at_ms"], OCT_1_0900_UTC);
    assert_eq!(launch[0]["metadata"]["modified_at_ms"], OCT_27_1846_UTC);
    assert_eq!(
        launch[1]["content"]["text"],
        "<h1>Launch &lt;plan&gt;</h1>\n<p>Ship Friday</p>"
    );
    assert_eq!(launch[1]["content"]["media_type"], "text/html");
    let denied = of_object(&events, &format!("{CLOUD}/1002"));
    assert_eq!(
        (&denied[0]["outcome"], &denied[0]["reason"]),
        (&json!("failed"), &json!("access_denied"))
    );
    assert_eq!(denied[1]["type"], "presence");
    let oversized = of_object(&events, &format!("{CLOUD}/1003"));
    assert_eq!(
        (&oversized[0]["outcome"], &oversized[0]["reason"]),
        (&json!("skipped"), &json!("unsupported"))
    );
}

#[tokio::test]
async fn confluence_refuses_a_page_from_another_space() {
    let mut elsewhere = confluence_page("1001", "Elsewhere", "<p>Not ours</p>");
    elsewhere["spaceId"] = "777".into();
    let server = Server::start_as(
        "confluence",
        ACCOUNT,
        vec![
            me(),
            json!({"results":[{"id":"1001"}],"_links":{}}),
            elsewhere,
        ],
        json!({"containers":[format!("{CLOUD}~98304")]}),
    )
    .await;

    assert!(server.run(&Confluence).await.is_err());
    assert!(server.events().await.is_empty());
}

fn paragraph(text: &str) -> Value {
    json!({"type":"doc","content":[{"type":"paragraph","content":[{"type":"text","text":text}]}]})
}

fn issue(id: &str, key: &str, summary: &str) -> Value {
    json!({"id":id,"key":key,"fields":{"summary":summary,"description":null,"status":{"name":"Done"},
        "issuetype":{"name":"Task"},"created":"2026-10-01T12:00:00.000+0300",
        "updated":"2026-10-27T18:46:00.000+0000","comment":{"comments":[]}}})
}

#[tokio::test]
async fn jira_issues_sync_with_description_and_comments_across_search_pages() {
    let mut bug = issue("10001", "OPS-1", "Fix login");
    bug["fields"]["description"] = paragraph("Users see a 500.");
    bug["fields"]["status"]["name"] = "In Progress".into();
    bug["fields"]["issuetype"]["name"] = "Bug".into();
    bug["fields"]["comment"]["comments"] = json!([{"author":{"displayName":"Ada"},
        "created":"2026-10-02T10:00:00.000+0000","body":paragraph("Fixed on staging.")}]);
    let server = Server::start_as(
        "jira",
        ACCOUNT,
        vec![
            me(),
            json!({"issues":[bug, issue("10002", "OPS-2", "Second")],"nextPageToken":"t2","isLast":false}),
            json!({"issues":[issue("10003", "OPS-3", "Third")],"isLast":true}),
        ],
        json!({"containers":[format!("{CLOUD}~OPS")]}),
    )
    .await;

    assert_eq!(server.run(&Jira).await.unwrap().state, "sealed");
    let operations = server.operations().await;
    let fields = "summary,description,status,issuetype,created,updated,comment";
    assert_eq!(
        operations,
        [
            ("me.get".to_owned(), json!({})),
            (
                "issues.search".to_owned(),
                json!({"cloud":CLOUD,"project":"OPS","fields":fields,"maxResults":"4"})
            ),
            (
                "issues.search".to_owned(),
                json!({"cloud":CLOUD,"project":"OPS","fields":fields,"maxResults":"4","nextPageToken":"t2"})
            ),
        ]
    );

    let events = server.events().await;
    let login = of_object(&events, &format!("{CLOUD}/10001"));
    assert_eq!(login[0]["metadata"]["created_at_ms"], OCT_1_0900_UTC);
    assert_eq!(login[0]["metadata"]["modified_at_ms"], OCT_27_1846_UTC);
    assert_eq!(
        login[1]["content"]["text"],
        "OPS-1: Fix login\nType: Bug · Status: In Progress\n\nUsers see a 500.\n\nComments\n\nAda (2026-10-02):\nFixed on staging.\n"
    );
    assert_eq!(
        of_object(&events, &format!("{CLOUD}/10003"))[1]["content"]["text"],
        "OPS-3: Third\nType: Task · Status: Done\n"
    );
}

#[tokio::test]
async fn jira_refuses_an_issue_from_another_project_and_a_token_for_another_account() {
    let server = Server::start_as(
        "jira",
        ACCOUNT,
        vec![
            me(),
            json!({"issues":[issue("10001", "SEC-1", "Not ours")],"isLast":true}),
        ],
        json!({"containers":[format!("{CLOUD}~OPS")]}),
    )
    .await;
    assert!(server.run(&Jira).await.is_err());
    assert!(server.events().await.is_empty());

    let server = Server::start_as(
        "jira",
        ACCOUNT,
        vec![json!({"account_id":"557058:someone-else"})],
        json!({"containers":[format!("{CLOUD}~OPS")]}),
    )
    .await;
    assert!(server.run(&Jira).await.is_err());
    assert_eq!(server.operations().await.len(), 1);
}
