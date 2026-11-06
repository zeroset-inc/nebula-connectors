use std::collections::HashSet;

use nebula_connector_sdk::{
    ClientError, Connector, ConnectorError, Context, Page, Specification,
    client::ProviderFailureCode,
    connector::{Capabilities, Resource, ResourcePage, stable_id},
    protocol::{ConnectorEvent, ItemMetadata},
    scope::Selection,
};
use serde_json::{Value, json};

use crate::{
    atlassian::{check_account, next_cursor, split, validate_context},
    common::{Cursor, Response, Result, broker_read, observed_now, request, timestamp_ms},
};

/// Pages per listing. Each costs two events and up to `MAX_TEXT_BYTES`, so
/// four keep a page within the runner's 1 MiB batch.
const PAGE_SIZE: usize = 4;
const MAX_TEXT_BYTES: usize = 96 << 10;
const MAX_ENCODED_TEXT: usize = 200 << 10;
const REPRESENTATION: &str = "confluence-page-v1";
const CONTENT_TYPE: &str = "application/vnd.atlassian.confluence.page";

/// Managed Confluence space inventories: every current page in a selected
/// space, as its stored XHTML body. A container names the site and space,
/// "cloud~space".
pub struct Confluence;

impl Connector for Confluence {
    fn specification(&self) -> Specification {
        Specification {
            provider: "confluence".into(),
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
        Box::pin(check_account(context))
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
                .and_then(|cursor| cursor["space"].as_u64())
                .unwrap_or_default() as usize;
            let spaces = context.scope.containers();
            let container = spaces.get(position).ok_or(ConnectorError::CursorExpired)?;
            let (cloud, space) = split(container)?;
            let info = request(context, "spaces.get", json!({"cloud":cloud,"space":space})).await?;
            let name = info["name"]
                .as_str()
                .filter(|name| !name.is_empty() && name.len() <= 1024)
                .ok_or(ConnectorError::Response)?;
            Ok(ResourcePage {
                resources: vec![Resource {
                    id: container.clone(),
                    name: name.into(),
                    kind: "space".into(),
                    parent_id: Some(cloud.into()),
                }],
                next: (position + 1 < spaces.len()).then(|| json!({"space":position + 1})),
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

async fn read(context: &Context, value: &Value) -> Result<Page> {
    validate_context(context)?;
    let spaces = context.scope.containers();
    let mut cursor = Cursor::parse(value, spaces.len())?;
    let Some(container) = spaces.get(cursor.container) else {
        return page(cursor, vec![], true);
    };
    let (cloud, space) = split(container)?;
    let mut arguments =
        json!({"cloud":cloud,"space":space,"limit":PAGE_SIZE.to_string(),"status":"current"});
    if !cursor.page.is_empty() {
        arguments["cursor"] = cursor.page.clone().into();
    }
    let listing = request(context, "pages.list", arguments).await?;
    let pages = listing["results"]
        .as_array()
        .ok_or(ConnectorError::Response)?;
    if pages.len() > PAGE_SIZE {
        return Err(ConnectorError::Response);
    }
    let observed_at_ms = observed_now()?;
    let mut events = Vec::with_capacity(pages.len() * 2);
    let mut identities = HashSet::with_capacity(pages.len());
    for listed in pages {
        let id = listed["id"]
            .as_str()
            .filter(|id| {
                (1..=32).contains(&id.len()) && id.bytes().all(|byte| byte.is_ascii_digit())
            })
            .ok_or(ConnectorError::Response)?;
        if !identities.insert(id.to_owned()) {
            return Err(ConnectorError::Response);
        }
        events.extend(page_events(context, container, cloud, space, id, observed_at_ms).await?);
    }
    cursor.advance(next_cursor(&listing)?.as_ref())?;
    let complete = cursor.container == spaces.len();
    page(cursor, events, complete)
}

fn page(cursor: Cursor, events: Vec<ConnectorEvent>, complete: bool) -> Result<Page> {
    Ok(Page {
        cursor: serde_json::to_value(cursor).map_err(|_| ConnectorError::Response)?,
        events,
        complete,
    })
}

enum Fetched {
    Text(String),
    Skipped(&'static str),
    Failed(&'static str),
}

async fn page_events(
    context: &Context,
    container: &str,
    cloud: &str,
    space: &str,
    id: &str,
    observed_at_ms: i64,
) -> Result<Vec<ConnectorEvent>> {
    let read = match broker_read(
        context,
        "pages.get",
        json!({"cloud":cloud,"page":id,"body-format":"storage"}),
    )
    .await
    {
        Ok(response) => Ok(response.body),
        Err(ClientError::Provider(failure)) => Err(match (failure.code, failure.provider_status) {
            (ProviderFailureCode::ResponseLimit, _) => Fetched::Skipped("unsupported"),
            (_, 401) => return Err(ConnectorError::Permission),
            (_, 403) => Fetched::Failed("access_denied"),
            (_, 404) => Fetched::Failed("unavailable"),
            _ => Fetched::Failed("fetch_failed"),
        }),
        Err(error) => return Err(error.into()),
    };
    let object = format!("{cloud}/{id}");
    let mut metadata = json!({"container_id":container,"path":format!("{container}/{id}"),"content_type":CONTENT_TYPE});
    let mut version = 0;
    let mut title = String::new();
    let fetched = match read {
        Err(fetched) => fetched,
        Ok(found) => {
            // The broker binds listings to the space; a page read is checked here.
            if found["id"].as_str() != Some(id) || found["spaceId"].as_str() != Some(space) {
                return Err(ConnectorError::Response);
            }
            version = found["version"]["number"]
                .as_u64()
                .ok_or(ConnectorError::Response)?;
            title = found["title"]
                .as_str()
                .unwrap_or("Untitled")
                .chars()
                .take(1024)
                .collect();
            metadata["created_at_ms"] = timestamp_ms(
                found["createdAt"]
                    .as_str()
                    .ok_or(ConnectorError::Response)?,
            )?
            .into();
            metadata["modified_at_ms"] = timestamp_ms(
                found["version"]["createdAt"]
                    .as_str()
                    .ok_or(ConnectorError::Response)?,
            )?
            .into();
            let body = found["body"]["storage"]["value"]
                .as_str()
                .unwrap_or_default();
            let text = format!("<h1>{}</h1>\n{body}", escape(&title));
            if body.trim().is_empty() {
                Fetched::Skipped("empty")
            } else if body.len() > MAX_TEXT_BYTES
                || serde_json::to_string(&text)
                    .map_err(|_| ConnectorError::Response)?
                    .len()
                    > MAX_ENCODED_TEXT
            {
                Fetched::Skipped("unsupported")
            } else {
                Fetched::Text(text)
            }
        }
    };
    let revision = stable_id(REPRESENTATION, &[object.as_bytes(), &version.to_be_bytes()]);
    let source = json!({"authority":context.authority,"object_id":object,"revision":revision});
    let metadata: ItemMetadata =
        serde_json::from_value(metadata).map_err(|_| ConnectorError::Response)?;
    let selection = context.scope.select(&metadata);
    let observation_id = stable_id(
        "confluence-observation-v1",
        &[
            context.run.scan_id.as_bytes(),
            context.run.id.as_bytes(),
            &context.run.fence.to_be_bytes(),
            &context.run.next_sequence.to_be_bytes(),
            context.authority.as_bytes(),
            object.as_bytes(),
            revision.as_bytes(),
        ],
    );
    let (outcome, reason) = match (&fetched, selection) {
        (_, Selection::Excluded) => ("skipped", selection.reason()),
        (_, Selection::MissingMetadata) => ("failed", selection.reason()),
        (Fetched::Text(_), _) => ("fetched", "in_scope"),
        (Fetched::Skipped(reason), _) => ("skipped", *reason),
        (Fetched::Failed(reason), _) => ("failed", *reason),
    };
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
        let mut change = json!({"type":kind,"change_id":stable_id("confluence-change-v1", &[observation_id.as_bytes(),kind.as_bytes()]),
            "observation_id":observation_id,"source":source,"observed_at_ms":observed_at_ms});
        if let Fetched::Text(text) = fetched {
            change["permissions"] = json!({"state":"unavailable"});
            change["metadata"] =
                json!({"provider":"confluence","page_id":id,"space_id":space,"title":title});
            change["content"] = json!({"kind":"text","text":text,"media_type":"text/html",
                "representation_version":REPRESENTATION});
        }
        events.push(change);
    }
    events
        .into_iter()
        .map(|event| serde_json::from_value(event).map_err(|_| ConnectorError::Response))
        .collect()
}

fn escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}
