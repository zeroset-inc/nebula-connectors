use std::collections::HashSet;

use nebula_connector_sdk::{
    Connector, ConnectorError, Context, Page, Specification,
    connector::{Capabilities, Resource, ResourcePage},
    protocol::ConnectorEvent,
};
use serde_json::{Value, json};

use crate::{
    common::{Cursor, Response, Result, observed_now, request},
    files::{File, PAGE_SIZE, fetch_text, file_events},
    graph::{check_account, graph_id, graph_time, next_page, page_arguments, validate_context},
};

const ITEM_FIELDS: &str =
    "id,name,file,folder,size,createdDateTime,lastModifiedDateTime,cTag,webUrl";

/// Where a Microsoft file adapter reads: OneDrive selects folders in the
/// user's drive, SharePoint selects whole document libraries.
struct Files {
    provider: &'static str,
    container_argument: &'static str,
    container_kind: &'static str,
    get: &'static str,
    children: &'static str,
    content: &'static str,
    representation: &'static str,
}

const ONEDRIVE: Files = Files {
    provider: "onedrive",
    container_argument: "folder",
    container_kind: "folder",
    get: "items.get",
    children: "items.children",
    content: "items.content",
    representation: "onedrive-text-v1",
};

const SHAREPOINT: Files = Files {
    provider: "sharepoint",
    container_argument: "drive",
    container_kind: "library",
    get: "drives.get",
    children: "drives.root.children",
    content: "drives.items.content",
    representation: "sharepoint-text-v1",
};

/// Managed OneDrive inventories: the files directly inside each selected
/// folder. Plain, Markdown, CSV and HTML files are read as text, up to
/// 192 KiB; Office documents and other files are observed as unsupported
/// until file transfer exists.
pub struct OneDrive;

/// Managed SharePoint inventories: the files at the top of each selected
/// document library, read as OneDrive reads folders.
pub struct SharePoint;

macro_rules! file_connector {
    ($connector:ty, $files:expr) => {
        impl Connector for $connector {
            fn specification(&self) -> Specification {
                specification(&$files)
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
                Box::pin(discover(&$files, context, cursor))
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
                Box::pin(read(&$files, context, cursor))
            }
        }
    };
}

file_connector!(OneDrive, ONEDRIVE);
file_connector!(SharePoint, SHAREPOINT);

fn specification(files: &Files) -> Specification {
    Specification {
        provider: files.provider.into(),
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

async fn discover(
    files: &Files,
    context: &Context,
    cursor: Option<&Value>,
) -> Result<ResourcePage> {
    validate_context(context)?;
    let position = cursor
        .and_then(|cursor| cursor["container"].as_u64())
        .unwrap_or_default() as usize;
    let containers = context.scope.containers();
    let container = containers
        .get(position)
        .ok_or(ConnectorError::CursorExpired)?;
    let mut arguments = json!({"select":"id,name"});
    arguments[files.container_argument] = container.clone().into();
    let info = request(context, files.get, arguments).await?;
    if info["id"].as_str() != Some(container.as_str()) {
        return Err(ConnectorError::Response);
    }
    let name = info["name"]
        .as_str()
        .filter(|name| !name.is_empty() && name.len() <= 1024)
        .ok_or(ConnectorError::Response)?;
    Ok(ResourcePage {
        resources: vec![Resource {
            id: container.clone(),
            name: name.into(),
            kind: files.container_kind.into(),
            parent_id: None,
        }],
        next: (position + 1 < containers.len()).then(|| json!({"container":position + 1})),
    })
}

async fn read(files: &Files, context: &Context, value: &Value) -> Result<Page> {
    validate_context(context)?;
    let containers = context.scope.containers();
    let mut cursor = Cursor::parse(value, containers.len())?;
    let Some(container) = containers.get(cursor.container) else {
        return page(cursor, vec![], true);
    };
    let mut arguments = json!({"select":ITEM_FIELDS,"top":PAGE_SIZE.to_string()});
    arguments[files.container_argument] = container.clone().into();
    page_arguments(&cursor.page, &mut arguments)?;
    let listing = request(context, files.children, arguments).await?;
    let items = listing["value"]
        .as_array()
        .ok_or(ConnectorError::Response)?;
    if items.len() > PAGE_SIZE {
        return Err(ConnectorError::Response);
    }
    let observed_at_ms = observed_now()?;
    let mut events = Vec::with_capacity(items.len() * 2);
    let mut identities = HashSet::with_capacity(items.len());
    for item in items {
        let id = item["id"]
            .as_str()
            .filter(|id| graph_id(id))
            .ok_or(ConnectorError::Response)?;
        if !identities.insert(id.to_owned()) {
            return Err(ConnectorError::Response);
        }
        // Subfolders are selected on their own, as Drive's are.
        if item.get("folder").is_some_and(|folder| !folder.is_null()) {
            continue;
        }
        events.extend(item_events(files, context, container, id, item, observed_at_ms).await?);
    }
    cursor.advance(next_page(&listing)?.as_ref())?;
    let complete = cursor.container == containers.len();
    page(cursor, events, complete)
}

fn page(cursor: Cursor, events: Vec<ConnectorEvent>, complete: bool) -> Result<Page> {
    Ok(Page {
        cursor: serde_json::to_value(cursor).map_err(|_| ConnectorError::Response)?,
        events,
        complete,
    })
}

async fn item_events(
    files: &Files,
    context: &Context,
    container: &str,
    id: &str,
    item: &Value,
    observed_at_ms: i64,
) -> Result<Vec<ConnectorEvent>> {
    let name = item["name"]
        .as_str()
        .filter(|name| !name.is_empty() && name.len() <= 1024)
        .ok_or(ConnectorError::Response)?;
    let content_tag = item["cTag"]
        .as_str()
        .filter(|tag| !tag.is_empty() && tag.len() <= 512)
        .ok_or(ConnectorError::Response)?;
    let file = File {
        provider: files.provider,
        representation: files.representation,
        container,
        id,
        name,
        mime_type: Some(
            item["file"]["mimeType"]
                .as_str()
                .unwrap_or("application/octet-stream"),
        ),
        content_tag,
        size: item["size"].as_u64(),
        created_at_ms: Some(graph_time(&item["createdDateTime"])?),
        modified_at_ms: graph_time(&item["lastModifiedDateTime"])?,
        url: item["webUrl"].as_str(),
    };
    let mut arguments = json!({"file":id});
    if files.container_argument == "drive" {
        arguments["drive"] = container.into();
    }
    file_events(context, &file, observed_at_ms, || {
        fetch_text(context, files.content, arguments)
    })
    .await
}
