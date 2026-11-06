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
    common::{Cursor, Response, Result, broker_read, observed_now, request},
    graph::{check_account, graph_id, graph_time, next_page, page_arguments, validate_context},
};

/// Messages per page. Each costs two events and up to `MAX_TEXT_BYTES` of
/// content, so four keep a page within the runner's 1 MiB batch.
const PAGE_SIZE: usize = 4;
const MAX_TEXT_BYTES: usize = 96 << 10;
const MAX_ENCODED_TEXT: usize = 200 << 10;
const REPRESENTATION: &str = "outlook-message-v1";
const CONTENT_TYPE: &str = "message/rfc822";

/// Managed Outlook mail folder inventories: every message in a selected
/// folder, as its plain text body behind its subject, sender, recipients and
/// date. Attachments are not read until file transfer exists.
pub struct Outlook;

impl Connector for Outlook {
    fn specification(&self) -> Specification {
        Specification {
            provider: "outlook".into(),
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
                .and_then(|cursor| cursor["folder"].as_u64())
                .unwrap_or_default() as usize;
            let folders = context.scope.containers();
            let folder = folders.get(position).ok_or(ConnectorError::CursorExpired)?;
            let info = request(
                context,
                "mailFolders.get",
                json!({"folder":folder,"select":"id,displayName"}),
            )
            .await?;
            let name = info["displayName"]
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
    let mut arguments = json!({"folder":folder,"select":"id","top":PAGE_SIZE.to_string()});
    page_arguments(&cursor.page, &mut arguments)?;
    let listing = request(context, "messages.list", arguments).await?;
    let messages = listing["value"]
        .as_array()
        .ok_or(ConnectorError::Response)?;
    if messages.len() > PAGE_SIZE {
        return Err(ConnectorError::Response);
    }
    let observed_at_ms = observed_now()?;
    let mut events = Vec::with_capacity(messages.len() * 2);
    let mut identities = HashSet::with_capacity(messages.len());
    for message in messages {
        let id = message["id"]
            .as_str()
            .filter(|id| graph_id(id))
            .ok_or(ConnectorError::Response)?;
        if !identities.insert(id.to_owned()) {
            return Err(ConnectorError::Response);
        }
        events.extend(message_events(context, folder, id, observed_at_ms).await?);
    }
    cursor.advance(next_page(&listing)?.as_ref())?;
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

fn address(value: &Value) -> String {
    let address = &value["emailAddress"];
    match (address["name"].as_str(), address["address"].as_str()) {
        (Some(name), Some(email)) if !name.is_empty() => format!("{name} <{email}>"),
        (_, Some(email)) => email.to_owned(),
        _ => String::new(),
    }
}

enum Fetched {
    Text(String),
    Skipped(&'static str),
    Failed(&'static str),
}

async fn message_events(
    context: &Context,
    folder: &str,
    id: &str,
    observed_at_ms: i64,
) -> Result<Vec<ConnectorEvent>> {
    let fields =
        "id,subject,from,toRecipients,receivedDateTime,lastModifiedDateTime,changeKey,body,webLink";
    let message = match broker_read(
        context,
        "messages.get",
        json!({"message":id,"select":fields}),
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
    let mut metadata =
        json!({"container_id":folder,"path":format!("{folder}/{id}"),"content_type":CONTENT_TYPE});
    let mut subject = String::new();
    let mut change_key = String::new();
    let mut link = None;
    let fetched = match message {
        Err(fetched) => fetched,
        Ok(message) => {
            if message["id"].as_str() != Some(id) {
                return Err(ConnectorError::Response);
            }
            metadata["created_at_ms"] = graph_time(&message["receivedDateTime"])?.into();
            metadata["modified_at_ms"] = graph_time(&message["lastModifiedDateTime"])?.into();
            change_key = message["changeKey"]
                .as_str()
                .unwrap_or_default()
                .chars()
                .take(512)
                .collect();
            link = message["webLink"]
                .as_str()
                .filter(|link| link.starts_with("https://") && link.len() <= 8192)
                .map(str::to_owned);
            subject = message["subject"]
                .as_str()
                .unwrap_or_default()
                .chars()
                .take(1024)
                .collect();
            let body = message["body"]["content"].as_str().unwrap_or_default();
            let recipients = message["toRecipients"]
                .as_array()
                .map(|to| to.iter().map(address).collect::<Vec<_>>().join(", "))
                .unwrap_or_default();
            let text = format!(
                "Subject: {subject}\nFrom: {}\nTo: {recipients}\nDate: {}\n\n{body}",
                address(&message["from"]),
                message["receivedDateTime"].as_str().unwrap_or_default()
            );
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
    let revision = stable_id(REPRESENTATION, &[id.as_bytes(), change_key.as_bytes()]);
    let mut source = json!({"authority":context.authority,"object_id":id,"revision":revision});
    if let Some(link) = link {
        source["url"] = link.into();
    }
    let metadata: ItemMetadata =
        serde_json::from_value(metadata).map_err(|_| ConnectorError::Response)?;
    let selection = context.scope.select(&metadata);
    let observation_id = stable_id(
        "outlook-observation-v1",
        &[
            context.run.scan_id.as_bytes(),
            context.run.id.as_bytes(),
            &context.run.fence.to_be_bytes(),
            &context.run.next_sequence.to_be_bytes(),
            context.authority.as_bytes(),
            id.as_bytes(),
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
        let mut change = json!({"type":kind,"change_id":stable_id("outlook-change-v1", &[observation_id.as_bytes(),kind.as_bytes()]),
            "observation_id":observation_id,"source":source,"observed_at_ms":observed_at_ms});
        if let Fetched::Text(text) = fetched {
            change["permissions"] = json!({"state":"unavailable"});
            change["metadata"] =
                json!({"provider":"outlook","message_id":id,"folder_id":folder,"subject":subject});
            change["content"] = json!({"kind":"text","text":text,"media_type":"text/plain",
                "representation_version":REPRESENTATION});
        }
        events.push(change);
    }
    events
        .into_iter()
        .map(|event| serde_json::from_value(event).map_err(|_| ConnectorError::Response))
        .collect()
}
