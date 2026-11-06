use std::{future::Future, pin::Pin};

use nebula_connector_sdk::{
    ClientError, Connector, ConnectorError, Context, Page, Specification,
    client::CredentialMode,
    connector::{Capabilities, Resource, ResourcePage, stable_id},
    protocol::{ConnectorEvent, ItemMetadata},
    scope::Selection,
};
use serde_json::{Value, json};

use crate::common::{Response, Result, broker_read, observed_now, request, timestamp_ms};

/// A page's text is skipped beyond this size rather than cut short.
const MAX_TEXT_BYTES: usize = 192 << 10;
/// Block reads per page; deeper or larger pages are skipped.
const MAX_BLOCK_CALLS: usize = 60;
const MAX_DEPTH: usize = 4;
const REPRESENTATION: &str = "notion-page-v1";
const CONTENT_TYPE: &str = "application/vnd.notion.page";

/// Managed Notion page inventories: each selected page becomes one text
/// source built from its blocks. Subpages appear as links and sync only when
/// selected themselves; archived pages are left out so they are withdrawn.
pub struct Notion;

impl Connector for Notion {
    fn specification(&self) -> Specification {
        Specification {
            provider: "notion".into(),
            version: env!("CARGO_PKG_VERSION").into(),
            cursor_version: 1,
            config_schema: json!({"type":"object","properties":{},"additionalProperties":false}),
            capabilities: Capabilities {
                managed: true,
                incremental: false,
                complete_inventory: true,
                permissions: false,
                original_files: false,
                supported_filters: ["container", "created_at", "modified_at"]
                    .map(str::to_owned)
                    .into(),
            },
        }
    }

    fn check<'life0, 'life1, 'async_trait>(
        &'life0 self,
        context: &'life1 Context,
    ) -> Response<'async_trait, ()>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            validate_context(context)?;
            let me = request(context, "users.me", json!({})).await?;
            if me["object"] != "user" {
                return Err(ConnectorError::Response);
            }
            Ok(())
        })
    }

    fn discover<'life0, 'life1, 'life2, 'async_trait>(
        &'life0 self,
        context: &'life1 Context,
        cursor: Option<&'life2 Value>,
    ) -> Response<'async_trait, ResourcePage>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            validate_context(context)?;
            let position = cursor
                .and_then(|cursor| cursor["page"].as_u64())
                .unwrap_or_default() as usize;
            let pages = context.scope.containers();
            let page = pages.get(position).ok_or(ConnectorError::CursorExpired)?;
            let info = request(context, "pages.get", json!({"page":page})).await?;
            Ok(ResourcePage {
                resources: vec![Resource {
                    id: page.clone(),
                    name: title(&info),
                    kind: "page".into(),
                    parent_id: None,
                }],
                next: (position + 1 < pages.len()).then(|| json!({"page":position + 1})),
            })
        })
    }

    fn read<'life0, 'life1, 'life2, 'async_trait>(
        &'life0 self,
        context: &'life1 Context,
        cursor: &'life2 Value,
    ) -> Response<'async_trait, Page>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(read(context, cursor))
    }
}

/// Notion IDs are UUIDs, written with or without hyphens.
fn notion_id(value: &str) -> bool {
    let hex = value.bytes().filter(|byte| *byte != b'-').count();
    hex == 32
        && value.len() <= 36
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase() || byte == b'-')
}

fn validate_context(context: &Context) -> Result<()> {
    if context.credential_mode != CredentialMode::Managed
        || !context.complete_inventory
        || context.config != json!({})
        || !notion_id(&context.authority)
        || context.scope.containers().iter().any(|id| !notion_id(id))
    {
        return Err(ConnectorError::Configuration);
    }
    Ok(())
}

fn plain(rich_text: &Value) -> String {
    rich_text
        .as_array()
        .map(|parts| {
            parts
                .iter()
                .filter_map(|part| part["plain_text"].as_str())
                .collect()
        })
        .unwrap_or_default()
}

fn title(page: &Value) -> String {
    page["properties"]
        .as_object()
        .and_then(|properties| {
            properties
                .values()
                .find(|property| property["type"] == "title")
        })
        .map(|property| plain(&property["title"]))
        .filter(|title| !title.trim().is_empty())
        .unwrap_or_else(|| "Untitled".into())
        .chars()
        .take(1024)
        .collect()
}

/// One block as a Markdown line, or None for blocks without text.
fn block_line(block: &Value) -> Option<String> {
    let kind = block["type"].as_str()?;
    let body = &block[kind];
    let text = plain(&body["rich_text"]);
    Some(match kind {
        "paragraph" | "toggle" | "callout" => text,
        "heading_1" => format!("# {text}"),
        "heading_2" => format!("## {text}"),
        "heading_3" => format!("### {text}"),
        "bulleted_list_item" => format!("- {text}"),
        "numbered_list_item" => format!("1. {text}"),
        "to_do" => format!(
            "- [{}] {text}",
            if body["checked"] == true { "x" } else { " " }
        ),
        "quote" => format!("> {text}"),
        "code" => format!("```\n{text}\n```"),
        "divider" => "---".into(),
        "child_page" => format!("[Page: {}]", body["title"].as_str().unwrap_or("Untitled")),
        "child_database" => format!(
            "[Database: {}]",
            body["title"].as_str().unwrap_or("Untitled")
        ),
        "bookmark" | "embed" | "link_preview" => body["url"].as_str()?.to_owned(),
        "table_row" => body["cells"]
            .as_array()?
            .iter()
            .map(plain)
            .collect::<Vec<_>>()
            .join(" | "),
        _ => return None,
    })
}

/// What reading a page's blocks produced.
enum Collected {
    Text,
    TooLarge,
    Failed(&'static str),
}

struct Collector {
    text: String,
    calls: usize,
}

impl Collector {
    /// Appends a block's children, depth first, paging through each list.
    fn children<'a>(
        &'a mut self,
        context: &'a Context,
        block: String,
        depth: usize,
    ) -> Pin<Box<dyn Future<Output = Result<Collected>> + Send + 'a>> {
        Box::pin(async move {
            let mut cursor = String::new();
            loop {
                if self.calls == MAX_BLOCK_CALLS || depth > MAX_DEPTH {
                    return Ok(Collected::TooLarge);
                }
                self.calls += 1;
                let mut arguments = json!({"block":block,"page_size":"100"});
                if !cursor.is_empty() {
                    arguments["start_cursor"] = cursor.clone().into();
                }
                let list = match broker_read(context, "blocks.children", arguments).await {
                    Ok(response) => response.body,
                    Err(ClientError::Provider(failure)) if failure.provider_status == 401 => {
                        return Err(ConnectorError::Permission);
                    }
                    Err(ClientError::Provider(failure)) if failure.provider_status == 404 => {
                        return Ok(Collected::Failed("unavailable"));
                    }
                    Err(ClientError::Provider(_)) => return Ok(Collected::Failed("fetch_failed")),
                    Err(error) => return Err(error.into()),
                };
                for block in list["results"].as_array().ok_or(ConnectorError::Response)? {
                    if let Some(line) = block_line(block) {
                        self.text.push_str(&"  ".repeat(depth));
                        self.text.push_str(&line);
                        self.text.push('\n');
                        if self.text.len() > MAX_TEXT_BYTES {
                            return Ok(Collected::TooLarge);
                        }
                    }
                    let kind = block["type"].as_str().unwrap_or_default();
                    if block["has_children"] == true
                        && kind != "child_page"
                        && kind != "child_database"
                    {
                        let id = block["id"]
                            .as_str()
                            .filter(|id| notion_id(id))
                            .ok_or(ConnectorError::Response)?;
                        match self.children(context, id.into(), depth + 1).await? {
                            Collected::Text => {}
                            other => return Ok(other),
                        }
                    }
                }
                match (list["has_more"].as_bool(), list["next_cursor"].as_str()) {
                    (Some(true), Some(next))
                        if !next.is_empty() && next != cursor && next.len() <= 1024 =>
                    {
                        cursor = next.into();
                    }
                    (Some(true), _) => return Err(ConnectorError::IncompleteInventory),
                    _ => return Ok(Collected::Text),
                }
            }
        })
    }
}

async fn read(context: &Context, value: &Value) -> Result<Page> {
    validate_context(context)?;
    let pages = context.scope.containers();
    let position = match value.get("page") {
        None => 0,
        Some(position) => position
            .as_u64()
            .filter(|position| *position as usize <= pages.len())
            .ok_or(ConnectorError::CursorExpired)? as usize,
    };
    let Some(page) = pages.get(position) else {
        return Ok(Page {
            cursor: json!({"page":position}),
            events: vec![],
            complete: true,
        });
    };
    let info = request(context, "pages.get", json!({"page":page})).await?;
    let events = if info["archived"] == true || info["in_trash"] == true {
        vec![]
    } else {
        page_events(context, page, &info).await?
    };
    Ok(Page {
        cursor: json!({"page":position + 1}),
        events,
        complete: position + 1 == pages.len(),
    })
}

async fn page_events(context: &Context, page: &str, info: &Value) -> Result<Vec<ConnectorEvent>> {
    let title = title(info);
    let created = timestamp_ms(
        info["created_time"]
            .as_str()
            .ok_or(ConnectorError::Response)?,
    )?;
    let modified = timestamp_ms(
        info["last_edited_time"]
            .as_str()
            .ok_or(ConnectorError::Response)?,
    )?;
    let revision = stable_id(REPRESENTATION, &[page.as_bytes(), &modified.to_be_bytes()]);
    let mut source = json!({"authority":context.authority,"object_id":page,"revision":revision});
    if let Some(link) = info["url"]
        .as_str()
        .filter(|link| link.starts_with("https://") && link.len() <= 8192)
    {
        source["url"] = link.into();
    }
    let metadata: ItemMetadata = serde_json::from_value(json!({"container_id":page,
        "path":format!("{page}/{title}"),"content_type":CONTENT_TYPE,"created_at_ms":created,"modified_at_ms":modified}))
    .map_err(|_| ConnectorError::Response)?;
    let selection = context.scope.select(&metadata);
    let observation_id = stable_id(
        "notion-observation-v1",
        &[
            context.run.scan_id.as_bytes(),
            context.run.id.as_bytes(),
            &context.run.fence.to_be_bytes(),
            &context.run.next_sequence.to_be_bytes(),
            context.authority.as_bytes(),
            page.as_bytes(),
            revision.as_bytes(),
        ],
    );
    let mut collector = Collector {
        text: format!("# {title}\n\n"),
        calls: 0,
    };
    // Blocks are read only for selected pages, so an exclusion never fetches.
    let collected = match selection {
        Selection::Selected => collector.children(context, page.into(), 0).await?,
        _ => Collected::Text,
    };
    let (outcome, reason) = match (selection, &collected) {
        (Selection::Excluded, _) => ("skipped", selection.reason()),
        (Selection::MissingMetadata, _) => ("failed", selection.reason()),
        (_, Collected::TooLarge) => ("skipped", "unsupported"),
        (_, Collected::Failed(reason)) => ("failed", *reason),
        (_, Collected::Text) => ("fetched", "in_scope"),
    };
    let observed_at_ms = observed_now()?;
    let mut events = vec![
        json!({"type":"item_observed","observation_id":observation_id,
        "scan_id":context.run.scan_id,"source":source,"observed_at_ms":observed_at_ms,
        "metadata":metadata,"selection":selection.selection(),"outcome":outcome,"reason":reason}),
    ];
    if selection == Selection::Selected {
        let kind = if outcome == "fetched" {
            "upsert"
        } else {
            "presence"
        };
        let mut change = json!({"type":kind,"change_id":stable_id("notion-change-v1", &[observation_id.as_bytes(),kind.as_bytes()]),
            "observation_id":observation_id,"source":source,"observed_at_ms":observed_at_ms});
        if kind == "upsert" {
            change["permissions"] = json!({"state":"unavailable"});
            change["metadata"] = json!({"provider":"notion","page_id":page,"title":title});
            change["content"] = json!({"kind":"text","text":collector.text,"media_type":"text/markdown",
                "representation_version":REPRESENTATION});
        }
        events.push(change);
    }
    events
        .into_iter()
        .map(|event| serde_json::from_value(event).map_err(|_| ConnectorError::Response))
        .collect()
}
