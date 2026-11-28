use std::collections::HashSet;

use nebula_connector_sdk::{
    Connector, ConnectorError, Context, Page, Specification,
    connector::{Capabilities, Resource, ResourcePage},
    protocol::ConnectorEvent,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{
    common::{Response, Result, observed_now, request},
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
    /// Lists a folder below a selected container. Without it, subfolders are
    /// selected on their own, as Drive's are.
    folder_children: Option<&'static str>,
    content: &'static str,
    representation: &'static str,
}

const ONEDRIVE: Files = Files {
    provider: "onedrive",
    container_argument: "folder",
    container_kind: "folder",
    get: "items.get",
    children: "items.children",
    folder_children: None,
    content: "items.content",
    representation: "onedrive-text-v1",
};

const SHAREPOINT: Files = Files {
    provider: "sharepoint",
    container_argument: "drive",
    container_kind: "library",
    get: "drives.get",
    children: "drives.root.children",
    folder_children: Some("drives.items.children"),
    content: "drives.items.content",
    representation: "sharepoint-text-v1",
};

/// Managed OneDrive inventories: the files directly inside each selected
/// folder. Plain, Markdown, CSV and HTML files are read as text, up to
/// 192 KiB; Office documents and other files are observed as unsupported
/// until file transfer exists.
pub struct OneDrive;

/// Managed SharePoint inventories: every file in each selected document
/// library, including the files in its folders, read as OneDrive reads them.
pub struct SharePoint;

/// Largest position a scan checkpoints, inside the runner's 64 KiB cursor.
const MAX_POSITION_BYTES: usize = 60 << 10;
/// Longest folder path kept for a file's observed path. With the container
/// ID and the file name, each at most 1,024 bytes, the path stays inside the
/// protocol's 8,192-character metadata path.
const MAX_FOLDER_PATH: usize = 4096;

/// Whether a stored page is one a listing can ask for again: none for a
/// first page, or the "skiptoken=…" `next_page` returns, within the size the
/// listing operations take.
fn listable(page: &str) -> bool {
    page.strip_prefix("skiptoken=")
        .map_or(page.is_empty(), |token| {
            !token.is_empty() && token.len() <= crate::graph::MAX_PAGE_TOKEN
        })
}

/// A folder below a selected container whose listing is unfinished.
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct OpenFolder {
    id: String,
    /// The folder's names from the container down, such as "Reports/2026".
    path: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    page: String,
}

/// Where a scan stands: the selected container, the next page of its top
/// level, and the folders below it still to list, deepest last. Folders are
/// listed before the level above continues, so each level of nesting holds at
/// most its own next page and the subfolders of one listing page: the
/// position grows with depth, not with the size of the library.
#[derive(Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
struct Position {
    container: usize,
    page: String,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    top_listed: bool,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    folders: Vec<OpenFolder>,
}

impl Position {
    fn parse(value: &Value, containers: usize, reads_folders: bool) -> Result<Self> {
        let position: Self =
            serde_json::from_value(value.clone()).map_err(|_| ConnectorError::CursorExpired)?;
        let finished = position.container == containers;
        let below_top = position.top_listed || !position.folders.is_empty();
        if position.container > containers
            || !listable(&position.page)
            || finished && !position.page.is_empty()
            || below_top && (finished || !reads_folders)
            || position.top_listed && (!position.page.is_empty() || position.folders.is_empty())
            || position.folders.iter().any(|folder| {
                !graph_id(&folder.id)
                    || folder.path.is_empty()
                    || folder.path.len() > MAX_FOLDER_PATH
                    || !listable(&folder.page)
            })
        {
            return Err(ConnectorError::CursorExpired);
        }
        Ok(position)
    }

    /// Records the listing just read: its next page, or its end, and the
    /// subfolders it found. Moves to the next container once nothing is left.
    fn advance(&mut self, next: Option<&Value>, found: Vec<OpenFolder>) -> Result<()> {
        let listed = self
            .folders
            .last()
            .map_or(&self.page, |folder| &folder.page);
        let next = match next {
            None => String::new(),
            Some(Value::String(token))
                if !token.is_empty() && listable(token) && token != listed =>
            {
                token.clone()
            }
            _ => return Err(ConnectorError::IncompleteInventory),
        };
        match self.folders.last_mut() {
            Some(folder) if !next.is_empty() => folder.page = next,
            Some(_) => {
                self.folders.pop();
            }
            None => {
                self.top_listed = next.is_empty();
                self.page = next;
            }
        }
        self.folders.extend(found);
        let held = serde_json::to_vec(self).map_err(|_| ConnectorError::Response)?;
        if held.len() > MAX_POSITION_BYTES {
            return Err(ConnectorError::IncompleteInventory);
        }
        if self.top_listed && self.folders.is_empty() {
            *self = Self {
                container: self.container + 1,
                ..Self::default()
            };
        }
        Ok(())
    }
}

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
    let mut position = Position::parse(value, containers.len(), files.folder_children.is_some())?;
    let Some(container) = containers.get(position.container) else {
        return page(position, vec![], true);
    };
    let mut arguments = json!({"select":ITEM_FIELDS,"top":PAGE_SIZE.to_string()});
    arguments[files.container_argument] = container.clone().into();
    let (operation, folder_path) = match (position.folders.last(), files.folder_children) {
        (Some(folder), Some(operation)) => {
            arguments["folder"] = folder.id.clone().into();
            page_arguments(&folder.page, &mut arguments)?;
            (operation, folder.path.clone())
        }
        _ => {
            page_arguments(&position.page, &mut arguments)?;
            (files.children, String::new())
        }
    };
    let listing = request(context, operation, arguments).await?;
    let items = listing["value"]
        .as_array()
        .ok_or(ConnectorError::Response)?;
    if items.len() > PAGE_SIZE {
        return Err(ConnectorError::Response);
    }
    let observed_at_ms = observed_now()?;
    let mut events = Vec::with_capacity(items.len() * 2);
    let mut identities = HashSet::with_capacity(items.len());
    let mut subfolders = Vec::new();
    for item in items {
        let id = item["id"]
            .as_str()
            .filter(|id| graph_id(id))
            .ok_or(ConnectorError::Response)?;
        if !identities.insert(id.to_owned()) {
            return Err(ConnectorError::Response);
        }
        if item.get("folder").is_some_and(|folder| !folder.is_null()) {
            if files.folder_children.is_some() {
                subfolders.push(OpenFolder {
                    id: id.to_owned(),
                    path: folder_below(&folder_path, item_name(item)?)?,
                    page: String::new(),
                });
            }
            continue;
        }
        let place = Place {
            container,
            folder: &folder_path,
        };
        events.extend(item_events(files, context, place, id, item, observed_at_ms).await?);
    }
    position.advance(next_page(&listing)?.as_ref(), subfolders)?;
    let complete = position.container == containers.len();
    page(position, events, complete)
}

fn item_name(item: &Value) -> Result<&str> {
    item["name"]
        .as_str()
        .filter(|name| !name.is_empty() && name.len() <= 1024)
        .ok_or(ConnectorError::Response)
}

fn folder_below(parent: &str, name: &str) -> Result<String> {
    let path = if parent.is_empty() {
        name.to_owned()
    } else {
        format!("{parent}/{name}")
    };
    if path.len() > MAX_FOLDER_PATH {
        return Err(ConnectorError::Response);
    }
    Ok(path)
}

/// Where a listed file sits: its selected container and the folders below it.
#[derive(Clone, Copy)]
struct Place<'a> {
    container: &'a str,
    folder: &'a str,
}

fn page(position: Position, events: Vec<ConnectorEvent>, complete: bool) -> Result<Page> {
    Ok(Page {
        cursor: serde_json::to_value(position).map_err(|_| ConnectorError::Response)?,
        events,
        complete,
    })
}

async fn item_events(
    files: &Files,
    context: &Context,
    place: Place<'_>,
    id: &str,
    item: &Value,
    observed_at_ms: i64,
) -> Result<Vec<ConnectorEvent>> {
    let name = item_name(item)?;
    let content_tag = item["cTag"]
        .as_str()
        .filter(|tag| !tag.is_empty() && tag.len() <= 512)
        .ok_or(ConnectorError::Response)?;
    let file = File {
        provider: files.provider,
        representation: files.representation,
        container: place.container,
        folder: place.folder,
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
        arguments["drive"] = place.container.into();
    }
    file_events(context, &file, observed_at_ms, |max_bytes| {
        fetch_text(context, files.content, arguments, max_bytes)
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn page(token_bytes: usize) -> String {
        format!("skiptoken={}", "t".repeat(token_bytes))
    }

    #[test]
    fn a_stored_page_fits_the_listing_operations() {
        for files in [ONEDRIVE, SHAREPOINT] {
            let catalog = format!(
                "{}/policies/{}-v1.json",
                env!("CARGO_MANIFEST_DIR"),
                files.provider
            );
            let catalog: Value =
                serde_json::from_str(&std::fs::read_to_string(catalog).unwrap()).unwrap();
            for listing in std::iter::once(files.children).chain(files.folder_children) {
                let operation = catalog["operations"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|operation| operation["id"] == listing)
                    .unwrap();
                assert_eq!(
                    operation["arguments"]["properties"]["skiptoken"]["maxLength"],
                    crate::graph::MAX_PAGE_TOKEN,
                    "{listing}"
                );
            }
        }
    }

    #[test]
    fn a_position_holds_only_pages_a_listing_can_ask_for() {
        let longest = page(crate::graph::MAX_PAGE_TOKEN);
        let folder = |page: &str| json!({"container":0,"page":"","folders":[{"id":"D1","path":"Reports","page":page}]});
        for position in [json!({"container":0,"page":longest}), folder(&longest)] {
            assert!(Position::parse(&position, 1, true).is_ok());
        }
        for unusable in [
            page(crate::graph::MAX_PAGE_TOKEN + 1),
            page(0),
            "skip=2".into(),
        ] {
            for position in [json!({"container":0,"page":unusable}), folder(&unusable)] {
                assert!(
                    matches!(
                        Position::parse(&position, 1, true),
                        Err(ConnectorError::CursorExpired)
                    ),
                    "{unusable}"
                );
            }
        }
    }
}
