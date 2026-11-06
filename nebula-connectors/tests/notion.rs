mod common;

use common::{Server, of_object};
use nebula_connectors::notion::Notion;
use serde_json::{Value, json};

const WORKSPACE: &str = "0f6c3d4e-1b2a-4c5d-8e9f-a0b1c2d3e4f5";
const PAGE: &str = "11111111-1111-1111-1111-111111111111";
const ARCHIVED: &str = "22222222-2222-2222-2222-222222222222";
const LIST: &str = "33333333-3333-3333-3333-333333333333";

fn text(kind: &str, content: &str) -> Value {
    json!({"id":"44444444-4444-4444-4444-444444444444","type":kind,"has_children":false,
        kind:{"rich_text":[{"plain_text":content}]}})
}

fn page(id: &str, title: &str, archived: bool) -> Value {
    json!({"object":"page","id":id,"archived":archived,"in_trash":false,"created_time":"2026-10-01T09:00:00.000Z",
        "last_edited_time":"2026-10-27T18:46:00.000Z","url":format!("https://www.notion.so/{}", id.replace('-', "")),
        "properties":{"title":{"type":"title","title":[{"plain_text":title}]}}})
}

#[tokio::test]
async fn selected_pages_become_markdown_from_their_blocks_and_archived_pages_are_left_out() {
    let mut list_item = text("bulleted_list_item", "Ship Friday");
    list_item["id"] = LIST.into();
    list_item["has_children"] = true.into();
    let server = Server::start_as(
        "notion",
        WORKSPACE,
        vec![
            json!({"object":"user","type":"bot"}),
            page(PAGE, "Launch plan", false),
            json!({"results":[text("heading_2","Goals"),list_item,
                {"id":"55555555-5555-5555-5555-555555555555","type":"child_page","has_children":true,"child_page":{"title":"Notes"}}],
                "has_more":true,"next_cursor":"c2"}),
            json!({"results":[text("to_do","Write docs")],"has_more":false,"next_cursor":null}),
            json!({"results":[text("paragraph","Revenue up 4%")],"has_more":false,"next_cursor":null}),
            page(ARCHIVED, "Old", true),
        ],
        json!({"containers":[PAGE, ARCHIVED]}),
    )
    .await;

    assert_eq!(server.run(&Notion).await.unwrap().state, "sealed");
    let operations = server.operations().await;
    assert_eq!(
        operations
            .iter()
            .map(|(op, _)| op.as_str())
            .collect::<Vec<_>>(),
        [
            "users.me",
            "pages.get",
            "blocks.children",
            "blocks.children",
            "blocks.children",
            "pages.get"
        ]
    );
    assert_eq!(operations[3].1, json!({"block":LIST,"page_size":"100"}));
    assert_eq!(
        operations[4].1,
        json!({"block":PAGE,"page_size":"100","start_cursor":"c2"})
    );
    let events = server.events().await;
    let launch = of_object(&events, PAGE);
    assert_eq!(
        launch[1]["content"]["text"],
        "# Launch plan\n\n## Goals\n- Ship Friday\n  - [ ] Write docs\n[Page: Notes]\nRevenue up 4%\n"
    );
    assert_eq!(launch[1]["content"]["media_type"], "text/markdown");
    assert!(of_object(&events, ARCHIVED).is_empty());
}
