use std::collections::HashSet;

use nebula_connector_sdk::{
    Connector, ConnectorError, Context, Page, Specification,
    client::CredentialMode,
    connector::{Capabilities, Resource, ResourcePage},
    protocol::ConnectorEvent,
};
use serde_json::{Value, json};

use crate::{
    common::{Cursor, Response, Result, observed_now, offset_timestamp_ms, request},
    files::{File, PAGE_SIZE, fetch_text, file_events},
};

const REPRESENTATION: &str = "box-text-v1";
const ITEM_FIELDS: &str = "id,type,name,size,sha1,created_at,modified_at";

/// Box user, file and folder IDs are numeric.
fn box_id(value: &str) -> bool {
    (1..=32).contains(&value.len()) && value.bytes().all(|byte| byte.is_ascii_digit())
}

/// Managed Box folder inventories: the files directly inside each selected
/// folder. Plain text, Markdown and HTML files are read as they are; other
/// files are observed as unsupported until file transfer exists.
pub struct BoxFiles;

fn validate_context(context: &Context) -> Result<()> {
    if context.credential_mode != CredentialMode::Managed
        || !context.complete_inventory
        || context.config != json!({})
        || !box_id(&context.authority)
        || !context
            .scope
            .containers()
            .iter()
            .all(|folder| box_id(folder))
    {
        return Err(ConnectorError::Configuration);
    }
    Ok(())
}

impl Connector for BoxFiles {
    fn specification(&self) -> Specification {
        Specification {
            provider: "box".into(),
            version: env!("CARGO_PKG_VERSION").into(),
            cursor_version: 1,
            config_schema: json!({"type":"object","properties":{},"additionalProperties":false}),
            capabilities: Capabilities {
                managed: true,
                incremental: false,
                complete_inventory: true,
                permissions: false,
                original_files: false,
                supported_filters: ["container", "content_type", "created_at", "modified_at"]
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
            let me = request(context, "users.me", json!({"fields":"id"})).await?;
            if me["id"].as_str() != Some(context.authority.as_str()) {
                return Err(ConnectorError::Permission);
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
                .and_then(|cursor| cursor["folder"].as_u64())
                .unwrap_or_default() as usize;
            let folders = context.scope.containers();
            let folder = folders.get(position).ok_or(ConnectorError::CursorExpired)?;
            let info = request(
                context,
                "folders.get",
                json!({"folder":folder,"fields":"id,name"}),
            )
            .await?;
            if info["id"].as_str() != Some(folder.as_str()) {
                return Err(ConnectorError::Response);
            }
            let name = info["name"]
                .as_str()
                .filter(|name| !name.is_empty() && name.len() <= 1024)
                .ok_or(ConnectorError::Response)?;
            Ok(ResourcePage {
                resources: vec![Resource {
                    id: folder.clone(),
                    name: name.into(),
                    kind: "folder".into(),
                    parent_id: None,
                }],
                next: (position + 1 < folders.len()).then(|| json!({"folder":position + 1})),
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
    let folders = context.scope.containers();
    let mut cursor = Cursor::parse(value, folders.len())?;
    let Some(folder) = folders.get(cursor.container) else {
        return page(cursor, vec![], true);
    };
    let mut arguments = json!({"folder":folder,"fields":ITEM_FIELDS,
        "limit":PAGE_SIZE.to_string(),"usemarker":"true"});
    if !cursor.page.is_empty() {
        arguments["marker"] = cursor.page.clone().into();
    }
    let listing = request(context, "folders.items", arguments).await?;
    let entries = listing["entries"]
        .as_array()
        .ok_or(ConnectorError::Response)?;
    if entries.len() > PAGE_SIZE {
        return Err(ConnectorError::Response);
    }
    let observed_at_ms = observed_now()?;
    let mut events = Vec::with_capacity(entries.len() * 2);
    let mut identities = HashSet::with_capacity(entries.len());
    for entry in entries {
        let id = entry["id"]
            .as_str()
            .filter(|id| box_id(id))
            .ok_or(ConnectorError::Response)?;
        if !identities.insert(id.to_owned()) {
            return Err(ConnectorError::Response);
        }
        // Subfolders are selected on their own, as Drive's are; web links
        // have no content.
        if entry["type"] != "file" {
            continue;
        }
        events.extend(entry_events(context, folder, id, entry, observed_at_ms).await?);
    }
    let next = match &listing["next_marker"] {
        Value::Null => None,
        Value::String(marker) if marker.is_empty() => None,
        marker @ Value::String(_) => Some(marker),
        _ => return Err(ConnectorError::Response),
    };
    cursor.advance(next)?;
    let complete = cursor.container == folders.len();
    page(cursor, events, complete)
}

fn page(cursor: Cursor, events: Vec<ConnectorEvent>, complete: bool) -> Result<Page> {
    Ok(Page {
        cursor: serde_json::to_value(cursor).map_err(|_| ConnectorError::Response)?,
        events,
        complete,
    })
}

async fn entry_events(
    context: &Context,
    folder: &str,
    id: &str,
    entry: &Value,
    observed_at_ms: i64,
) -> Result<Vec<ConnectorEvent>> {
    let name = entry["name"]
        .as_str()
        .filter(|name| !name.is_empty() && name.len() <= 1024)
        .ok_or(ConnectorError::Response)?;
    let content_tag = entry["sha1"]
        .as_str()
        .filter(|sha1| sha1.len() == 40 && sha1.bytes().all(|byte| byte.is_ascii_hexdigit()))
        .ok_or(ConnectorError::Response)?;
    let time = |field: &str| {
        entry[field]
            .as_str()
            .ok_or(ConnectorError::Response)
            .and_then(offset_timestamp_ms)
    };
    let url = format!("https://app.box.com/file/{id}");
    let file = File {
        provider: "box",
        representation: REPRESENTATION,
        container: folder,
        id,
        name,
        mime_type: None,
        content_tag,
        size: entry["size"].as_u64(),
        created_at_ms: Some(time("created_at")?),
        modified_at_ms: time("modified_at")?,
        url: Some(&url),
    };
    file_events(context, &file, observed_at_ms, || {
        fetch_text(context, "files.content", json!({"file":id}))
    })
    .await
}
