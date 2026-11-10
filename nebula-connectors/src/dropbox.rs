use std::collections::HashSet;

use nebula_connector_sdk::{
    Connector, ConnectorError, Context, Page, Specification,
    client::CredentialMode,
    connector::{Capabilities, Resource, ResourcePage},
    protocol::ConnectorEvent,
};
use serde_json::{Value, json};

use crate::{
    common::{Cursor, Response, Result, observed_now, request, timestamp_ms},
    files::{Fetched, File, PAGE_SIZE, fetch_text, file_events},
};

const REPRESENTATION: &str = "dropbox-text-v1";

/// A stable Dropbox ID after its prefix, such as id:a4ayc_80_OEAAAAAAAAAXw or
/// dbid:AAH4f99T0taONIb-OurWxbNQ6ywGRopQngc.
fn dropbox_id(value: &str, prefix: &str) -> bool {
    value.strip_prefix(prefix).is_some_and(|rest| {
        (1..=128).contains(&rest.len())
            && rest
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"_-".contains(&byte))
    })
}

/// Managed Dropbox folder inventories: the files directly inside each
/// selected folder, addressed by folder ID so renames and moves keep the
/// selection. Plain text, Markdown and HTML files are read as they are; other
/// files are observed as unsupported until file transfer exists.
pub struct Dropbox;

fn validate_context(context: &Context) -> Result<()> {
    if context.credential_mode != CredentialMode::Managed
        || !context.complete_inventory
        || context.config != json!({})
        || !dropbox_id(&context.authority, "dbid:")
        || !context
            .scope
            .containers()
            .iter()
            .all(|folder| dropbox_id(folder, "id:"))
    {
        return Err(ConnectorError::Configuration);
    }
    Ok(())
}

impl Connector for Dropbox {
    fn specification(&self) -> Specification {
        Specification {
            provider: "dropbox".into(),
            version: env!("CARGO_PKG_VERSION").into(),
            cursor_version: 1,
            config_schema: json!({"type":"object","properties":{},"additionalProperties":false}),
            capabilities: Capabilities {
                managed: true,
                incremental: false,
                complete_inventory: true,
                permissions: false,
                original_files: false,
                supported_filters: ["container", "content_type", "modified_at"]
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
            let account = request(context, "users.get_current_account", json!({})).await?;
            if account["account_id"].as_str() != Some(context.authority.as_str()) {
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
            let info = request(context, "files.get_metadata", json!({"folder":folder})).await?;
            if info[".tag"] != "folder" || info["id"].as_str() != Some(folder.as_str()) {
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
    // The policy asks for one entry a page; Dropbox treats the limit as
    // approximate, and two still fit a batch.
    let listing = if cursor.page.is_empty() {
        request(context, "files.list_folder", json!({"folder":folder})).await?
    } else {
        request(
            context,
            "files.list_folder_continue",
            json!({"cursor":cursor.page}),
        )
        .await?
    };
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
        // Deleted entries appear only in incremental listings.
        if entry[".tag"] != "file" && entry[".tag"] != "folder" {
            return Err(ConnectorError::Response);
        }
        let id = entry["id"]
            .as_str()
            .filter(|id| dropbox_id(id, "id:"))
            .ok_or(ConnectorError::Response)?;
        if !identities.insert(id.to_owned()) {
            return Err(ConnectorError::Response);
        }
        // Subfolders are selected on their own, as Drive's are.
        if entry[".tag"] == "folder" {
            continue;
        }
        events.extend(entry_events(context, folder, id, entry, observed_at_ms).await?);
    }
    let next = match (&listing["has_more"], &listing["cursor"]) {
        (Value::Bool(false), _) => None,
        (Value::Bool(true), token @ Value::String(_)) => Some(token),
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
    let content_tag = entry["content_hash"]
        .as_str()
        .filter(|hash| hash.len() == 64 && hash.bytes().all(|byte| byte.is_ascii_hexdigit()))
        .ok_or(ConnectorError::Response)?;
    let modified = entry["server_modified"]
        .as_str()
        .ok_or(ConnectorError::Response)?;
    let file = File {
        provider: "dropbox",
        representation: REPRESENTATION,
        container: folder,
        id,
        name,
        mime_type: None,
        content_tag,
        size: entry["size"].as_u64(),
        created_at_ms: None,
        modified_at_ms: timestamp_ms(modified)?,
        url: None,
    };
    let downloadable = entry["is_downloadable"] != false;
    file_events(context, &file, observed_at_ms, |max_bytes| async move {
        if !downloadable {
            return Ok(Fetched::Skipped("unsupported"));
        }
        let arg = json!({"path":id}).to_string();
        fetch_text(context, "files.download", json!({"arg":arg}), max_bytes).await
    })
    .await
}
