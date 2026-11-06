use std::collections::HashSet;

use nebula_connector_sdk::{
    ClientError, Connector, ConnectorError, Context, Page, Specification,
    client::ProviderFailureCode,
    connector::{Capabilities, Resource, ResourcePage, stable_id},
    protocol::{ConnectorEvent, ItemMetadata},
    scope::Selection,
};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::{
    common::{Cursor, Response, Result, broker_read, observed_now, request, timestamp_ms},
    google::{google_id, validate_context},
};

/// Files per page. Each file costs two events and up to `MAX_TEXT_BYTES` of
/// content, so two keep a page within the runner's 1 MiB batch.
const PAGE_SIZE: usize = 2;
/// Largest export or plain file read; the catalog caps replies at this size.
const MAX_TEXT_BYTES: u64 = 192 << 10;
/// JSON escaping can grow text; larger encodings are skipped, not batched.
const MAX_ENCODED_TEXT: usize = 400 << 10;
const REPRESENTATION: &str = "google-drive-text-v1";
const CONTENT_REVISION: &str = "google-drive-content-v1";

/// Managed Google Drive folder inventories: the files directly inside each
/// selected folder. Google Docs, Sheets and Slides are exported as text, and
/// plain text files are read as they are; other files are observed as
/// unsupported until file transfer exists.
pub struct GoogleDrive;

impl Connector for GoogleDrive {
    fn specification(&self) -> Specification {
        Specification {
            provider: "google_drive".into(),
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
            let about =
                request(context, "about.get", json!({"fields":"user(permissionId)"})).await?;
            about["user"]["permissionId"]
                .as_str()
                .filter(|id| !id.is_empty())
                .ok_or(ConnectorError::Response)?;
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
            #[derive(Default, Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Discovery {
                #[serde(default)]
                folder: usize,
            }
            let position: Discovery = serde_json::from_value(cursor.cloned().unwrap_or(json!({})))
                .map_err(|_| ConnectorError::CursorExpired)?;
            let folders = context.scope.containers();
            let folder = folders
                .get(position.folder)
                .ok_or(ConnectorError::CursorExpired)?;
            let info = request(
                context,
                "files.get",
                json!({"folder":folder,"fields":"id,name,mimeType","supportsAllDrives":"true"}),
            )
            .await?;
            if info["id"].as_str() != Some(folder.as_str())
                || info["mimeType"] != "application/vnd.google-apps.folder"
            {
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
                next: (position.folder + 1 < folders.len())
                    .then(|| json!({"folder":position.folder + 1})),
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

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct File {
    id: String,
    name: String,
    mime_type: String,
    created_time: String,
    modified_time: String,
    version: String,
    #[serde(default)]
    size: Option<String>,
    #[serde(default)]
    web_view_link: Option<String>,
}

/// How a file's text is read: an export for Google formats, or the file
/// itself for plain text.
struct Text {
    operation: &'static str,
    export: Option<&'static str>,
    media_type: &'static str,
}

fn text_of(mime_type: &str) -> Option<Text> {
    let export = |export, media_type| Text {
        operation: "files.export",
        export: Some(export),
        media_type,
    };
    let download = |media_type| Text {
        operation: "files.download",
        export: None,
        media_type,
    };
    Some(match mime_type {
        "application/vnd.google-apps.document" | "application/vnd.google-apps.presentation" => {
            export("text/plain", "text/plain")
        }
        "application/vnd.google-apps.spreadsheet" => export("text/csv", "text/plain"),
        "text/plain" | "text/csv" | "text/tab-separated-values" => download("text/plain"),
        "text/markdown" | "text/x-markdown" => download("text/markdown"),
        "text/html" => download("text/html"),
        _ => return None,
    })
}

async fn read(context: &Context, value: &Value) -> Result<Page> {
    validate_context(context)?;
    let folders = context.scope.containers();
    let mut cursor = Cursor::parse(value, folders.len())?;
    let Some(folder) = folders.get(cursor.container) else {
        return page(cursor, vec![], true);
    };
    let mut arguments = json!({"folder":folder,"pageSize":PAGE_SIZE.to_string(),
        "fields":"nextPageToken,incompleteSearch,files(id,name,mimeType,createdTime,modifiedTime,version,size,webViewLink)",
        "supportsAllDrives":"true","includeItemsFromAllDrives":"true"});
    if !cursor.page.is_empty() {
        arguments["pageToken"] = cursor.page.clone().into();
    }
    let listing = request(context, "files.list", arguments).await?;
    if listing
        .get("incompleteSearch")
        .is_some_and(|value| value != false)
    {
        return Err(ConnectorError::IncompleteInventory);
    }
    let files: Vec<File> =
        serde_json::from_value(listing["files"].clone()).map_err(|_| ConnectorError::Response)?;
    if files.len() > PAGE_SIZE {
        return Err(ConnectorError::Response);
    }
    let observed_at_ms = observed_now()?;
    let mut events = Vec::with_capacity(files.len() * 2);
    let mut identities = HashSet::with_capacity(files.len());
    for file in &files {
        if !google_id(&file.id) || !identities.insert(file.id.as_str()) {
            return Err(ConnectorError::Response);
        }
        events.extend(file_events(context, folder, file, observed_at_ms).await?);
    }
    cursor.advance(listing.get("nextPageToken"))?;
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

/// What reading a selected file produced: its text, or why it has none.
enum Fetched {
    Text(String),
    Skipped(&'static str),
    Failed(&'static str),
}

async fn fetch(context: &Context, file: &File, text: &Text) -> Result<Fetched> {
    let mut arguments = json!({"file":file.id});
    if let Some(export) = text.export {
        arguments["mimeType"] = export.into();
    } else {
        arguments["alt"] = "media".into();
        arguments["supportsAllDrives"] = "true".into();
    }
    let body = match broker_read(context, text.operation, arguments).await {
        Ok(response) => response.body,
        Err(ClientError::Provider(failure)) => {
            return Ok(match (failure.code, failure.provider_status) {
                (ProviderFailureCode::ResponseLimit, _) => Fetched::Skipped("unsupported"),
                (ProviderFailureCode::InvalidResponse, _) => Fetched::Skipped("unsupported"),
                (_, 401) => return Err(ConnectorError::Permission),
                (_, 403) => Fetched::Failed("access_denied"),
                (_, 404) => Fetched::Failed("unavailable"),
                _ => Fetched::Failed("fetch_failed"),
            });
        }
        Err(error) => return Err(error.into()),
    };
    let Value::String(content) = body else {
        return Err(ConnectorError::Response);
    };
    if content.trim().is_empty() {
        return Ok(Fetched::Skipped("empty"));
    }
    if serde_json::to_string(&content)
        .map_err(|_| ConnectorError::Response)?
        .len()
        > MAX_ENCODED_TEXT
    {
        return Ok(Fetched::Skipped("unsupported"));
    }
    Ok(Fetched::Text(content))
}

async fn file_events(
    context: &Context,
    folder: &str,
    file: &File,
    observed_at_ms: i64,
) -> Result<Vec<ConnectorEvent>> {
    let created = timestamp_ms(&file.created_time)?;
    let modified = timestamp_ms(&file.modified_time)?;
    let size = match &file.size {
        None => None,
        Some(size) => Some(size.parse::<u64>().map_err(|_| ConnectorError::Response)?),
    };
    if file.name.is_empty()
        || file.name.len() > 1024
        || file.mime_type.is_empty()
        || file.mime_type.len() > 256
        || file.version.is_empty()
        || file.version.len() > 64
        || !file.version.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(ConnectorError::Response);
    }
    let mut metadata = json!({"container_id":folder,"path":format!("{folder}/{}", file.name),
        "content_type":file.mime_type,"created_at_ms":created,"modified_at_ms":modified});
    if let Some(size) = size {
        metadata["byte_length"] = size.into();
    }
    let metadata: ItemMetadata =
        serde_json::from_value(metadata).map_err(|_| ConnectorError::Response)?;
    let selection = context.scope.select(&metadata);
    let text = text_of(&file.mime_type);
    // Content is read only for selected files, so an exclusion never fetches.
    let fetched = match (selection, &text) {
        (Selection::Selected, Some(_)) if size.is_some_and(|size| size > MAX_TEXT_BYTES) => {
            Fetched::Skipped("unsupported")
        }
        (Selection::Selected, Some(text)) => fetch(context, file, text).await?,
        (Selection::Selected, None) => Fetched::Skipped("unsupported"),
        (_, _) => Fetched::Skipped(selection.reason()),
    };
    // Drive's version also advances on renames, moves and sharing changes, so
    // the revision of a file that was read is a digest of its text instead.
    let revision = match (&fetched, &text) {
        (Fetched::Text(content), Some(text)) => stable_id(
            CONTENT_REVISION,
            &[
                file.id.as_bytes(),
                text.media_type.as_bytes(),
                content.as_bytes(),
            ],
        ),
        _ => stable_id(
            REPRESENTATION,
            &[file.id.as_bytes(), file.version.as_bytes()],
        ),
    };
    let mut source = json!({"authority":context.authority,"object_id":file.id,"revision":revision});
    if let Some(link) = file
        .web_view_link
        .as_deref()
        .filter(|link| link.starts_with("https://") && link.len() <= 8192)
    {
        source["url"] = link.into();
    }
    let observation_id = stable_id(
        "google-drive-observation-v1",
        &[
            context.run.scan_id.as_bytes(),
            context.run.id.as_bytes(),
            &context.run.fence.to_be_bytes(),
            &context.run.next_sequence.to_be_bytes(),
            context.authority.as_bytes(),
            file.id.as_bytes(),
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
        let mut change = json!({"type":kind,"change_id":stable_id("google-drive-change-v1", &[observation_id.as_bytes(),kind.as_bytes()]),
            "observation_id":observation_id,"source":source,"observed_at_ms":observed_at_ms});
        if let Fetched::Text(content) = fetched {
            let media_type = text
                .map(|text| text.media_type)
                .ok_or(ConnectorError::Response)?;
            change["permissions"] = json!({"state":"unavailable"});
            change["metadata"] = json!({"provider":"google_drive","file_id":file.id,
                "name":file.name,"mime_type":file.mime_type,"folder_id":folder});
            change["content"] = json!({"kind":"text","text":content,"media_type":media_type,
                "representation_version":REPRESENTATION});
        }
        events.push(change);
    }
    events
        .into_iter()
        .map(|event| serde_json::from_value(event).map_err(|_| ConnectorError::Response))
        .collect()
}
