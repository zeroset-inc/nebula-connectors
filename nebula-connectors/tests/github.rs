mod common;

use common::{Server, of_object};
use nebula_connectors::github::GitHub;
use serde_json::{Value, json};

fn issue(id: u64, number: u64, title: &str) -> Value {
    json!({"id":id,"number":number,"title":title,"state":"open","body":null,"labels":[],"comments":0,
        "user":{"login":"ada"},"html_url":format!("https://github.com/acme/web/issues/{number}"),
        "created_at":"2026-10-01T09:00:00Z","updated_at":"2026-10-27T18:46:00Z"})
}

#[tokio::test]
async fn github_issues_and_pull_requests_sync_with_comments_page_by_page() {
    let mut pull = issue(9001, 1, "Fix login");
    pull["pull_request"] = json!({"url":"https://api.github.com/repos/acme/web/pulls/1"});
    pull["body"] = "Users see a 500.".into();
    pull["labels"] = json!([{"name":"bug"},{"name":"auth"}]);
    pull["state"] = "closed".into();
    pull["comments"] = 1.into();
    let server = Server::start_as(
        "github",
        "583231",
        vec![
            json!({"id":583231,"login":"ada"}),
            json!([pull, issue(9002, 2, "Two"), issue(9003, 3, "Three"), issue(9004, 4, "Four")]),
            json!([{"user":{"login":"grace"},"created_at":"2026-10-02T10:00:00Z","body":"Fixed on staging."}]),
            json!([issue(9005, 5, "Five")]),
        ],
        json!({"containers":["acme/web"]}),
    )
    .await;

    assert_eq!(server.run(&GitHub).await.unwrap().state, "sealed");
    let list = |page: &str| json!({"owner":"acme","repo":"web","state":"all","sort":"created","direction":"asc","per_page":"4","page":page});
    assert_eq!(
        server.operations().await,
        [
            ("user.get".to_owned(), json!({})),
            ("issues.list".to_owned(), list("1")),
            (
                "issues.comments".to_owned(),
                json!({"owner":"acme","repo":"web","number":"1","per_page":"20"})
            ),
            ("issues.list".to_owned(), list("2")),
        ]
    );
    let events = server.events().await;
    let login = of_object(&events, "9001");
    assert_eq!(
        login[1]["content"]["text"],
        "# #1 Fix login\n\nPull request · closed · labels: bug, auth · opened by ada\n\nUsers see a 500.\n\n## Comments\n\n**grace** (2026-10-02):\n\nFixed on staging.\n"
    );
    assert_eq!(login[0]["metadata"]["path"], "acme/web#1");
    assert_eq!(
        login[0]["metadata"]["content_type"],
        "application/vnd.github.pull-request"
    );
    assert_eq!(
        login[0]["source"]["url"],
        "https://github.com/acme/web/issues/1"
    );
    assert_eq!(
        of_object(&events, "9005")[1]["content"]["text"],
        "# #5 Five\n\nIssue · open · opened by ada\n"
    );
}

#[tokio::test]
async fn github_refuses_a_token_for_another_user() {
    let server = Server::start_as(
        "github",
        "583231",
        vec![json!({"id":1,"login":"someone"})],
        json!({"containers":["acme/web"]}),
    )
    .await;
    assert!(server.run(&GitHub).await.is_err());
    assert_eq!(server.operations().await.len(), 1);
}
