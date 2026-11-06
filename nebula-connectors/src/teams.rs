use nebula_connector_sdk::{
    Connector, ConnectorError, Context, Page, Specification,
    connector::{Capabilities, Resource, ResourcePage, stable_id},
    protocol::{ConnectorEvent, ItemMetadata},
    scope::Selection,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{
    common::{Response, Result, observed_now, request},
    graph::{check_account, graph_time, next_page, page_arguments, validate_context},
};

/// Replies read per page, within the runner's 98 events.
const REPLY_PAGE: usize = 40;
const MAX_TEXT_BYTES: usize = 48 << 10;
const REPRESENTATION: &str = "teams-message-v1";
const CONTENT_TYPE: &str = "application/vnd.microsoft.teams.message";

/// Managed Teams channel inventories: every message and reply in a selected
/// channel, each its own source. A container names the team and channel,
/// "team~channel", because Graph reads a channel only within its team.
pub struct Teams;

impl Connector for Teams {
    fn specification(&self) -> Specification {
        Specification {
            provider: "teams".into(),
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
                .and_then(|cursor| cursor["channel"].as_u64())
                .unwrap_or_default() as usize;
            let channels = context.scope.containers();
            let container = channels
                .get(position)
                .ok_or(ConnectorError::CursorExpired)?;
            let (team, channel) = split(container)?;
            let info = request(
                context,
                "channels.get",
                json!({"team":team,"channel":channel,"select":"id,displayName"}),
            )
            .await?;
            let name = info["displayName"]
                .as_str()
                .filter(|name| !name.is_empty() && name.len() <= 1024)
                .ok_or(ConnectorError::Response)?;
            Ok(ResourcePage {
                resources: vec![Resource {
                    id: container.clone(),
                    name: format!("#{name}"),
                    kind: "channel".into(),
                    parent_id: Some(team.into()),
                }],
                next: (position + 1 < channels.len()).then(|| json!({"channel":position + 1})),
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

fn split(container: &str) -> Result<(&str, &str)> {
    container
        .split_once('~')
        .filter(|(team, channel)| !team.is_empty() && !channel.is_empty() && !channel.contains('~'))
        .ok_or(ConnectorError::Configuration)
}

/// The position in a channel: its message pages, and the replies of the last
/// message read, which come before the next message.
#[derive(Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
struct Cursor {
    channel: usize,
    page: String,
    messages_done: bool,
    message: String,
    replies: String,
}

impl Cursor {
    fn parse(value: &Value, channels: usize) -> Result<Self> {
        let cursor: Self =
            serde_json::from_value(value.clone()).map_err(|_| ConnectorError::CursorExpired)?;
        if cursor.channel > channels
            || cursor.page.len() > 4096
            || cursor.replies.len() > 4096
            || cursor.message.len() > 64
            || !cursor.message.bytes().all(|byte| byte.is_ascii_digit())
            || cursor.message.is_empty() && !cursor.replies.is_empty()
        {
            return Err(ConnectorError::CursorExpired);
        }
        Ok(cursor)
    }

    fn next_channel(&mut self) {
        *self = Self {
            channel: self.channel + 1,
            ..Self::default()
        };
    }
}

fn token(next: Option<Value>) -> String {
    next.and_then(|next| next.as_str().map(str::to_owned))
        .unwrap_or_default()
}

async fn read(context: &Context, value: &Value) -> Result<Page> {
    validate_context(context)?;
    let channels = context.scope.containers();
    let mut cursor = Cursor::parse(value, channels.len())?;
    let Some(container) = channels.get(cursor.channel) else {
        return page(cursor, vec![], true);
    };
    let (team, channel) = split(container)?;
    let observed_at_ms = observed_now()?;
    let mut events = vec![];
    if !cursor.message.is_empty() {
        let mut arguments = json!({"team":team,"channel":channel,"message":cursor.message,"top":REPLY_PAGE.to_string()});
        page_arguments(&cursor.replies, &mut arguments)?;
        let replies = request(context, "replies.list", arguments).await?;
        let values = replies["value"]
            .as_array()
            .ok_or(ConnectorError::Response)?;
        if values.len() > REPLY_PAGE {
            return Err(ConnectorError::Response);
        }
        for reply in values {
            events.extend(message_events(context, container, reply, observed_at_ms)?);
        }
        let next = token(next_page(&replies)?);
        if !next.is_empty() && next == cursor.replies {
            return Err(ConnectorError::IncompleteInventory);
        }
        cursor.replies = next;
        if cursor.replies.is_empty() {
            cursor.message.clear();
        }
    } else if !cursor.messages_done {
        let mut arguments = json!({"team":team,"channel":channel,"top":"1"});
        page_arguments(&cursor.page, &mut arguments)?;
        let messages = request(context, "messages.list", arguments).await?;
        let values = messages["value"]
            .as_array()
            .ok_or(ConnectorError::Response)?;
        if values.len() > 1 {
            return Err(ConnectorError::Response);
        }
        if let Some(message) = values.first() {
            let id = message["id"].as_str().ok_or(ConnectorError::Response)?;
            if id.is_empty() || id.len() > 64 || !id.bytes().all(|byte| byte.is_ascii_digit()) {
                return Err(ConnectorError::Response);
            }
            events.extend(message_events(context, container, message, observed_at_ms)?);
            if message["deletedDateTime"].is_null() {
                cursor.message = id.into();
            }
        }
        let next = token(next_page(&messages)?);
        if !next.is_empty() && next == cursor.page {
            return Err(ConnectorError::IncompleteInventory);
        }
        cursor.messages_done = next.is_empty();
        cursor.page = next;
    }
    if cursor.messages_done && cursor.message.is_empty() {
        cursor.next_channel();
    }
    let complete = cursor.channel == channels.len();
    page(cursor, events, complete)
}

fn page(cursor: Cursor, events: Vec<ConnectorEvent>, complete: bool) -> Result<Page> {
    Ok(Page {
        cursor: serde_json::to_value(cursor).map_err(|_| ConnectorError::Response)?,
        events,
        complete,
    })
}

/// Events for one message or reply. Deleted messages produce none, so a
/// complete inventory withdraws them.
fn message_events(
    context: &Context,
    container: &str,
    message: &Value,
    observed_at_ms: i64,
) -> Result<Vec<ConnectorEvent>> {
    if !message["deletedDateTime"].is_null() {
        return Ok(vec![]);
    }
    let id = message["id"]
        .as_str()
        .filter(|id| {
            !id.is_empty() && id.len() <= 64 && id.bytes().all(|byte| byte.is_ascii_digit())
        })
        .ok_or(ConnectorError::Response)?;
    let created = graph_time(&message["createdDateTime"])?;
    let modified = match &message["lastModifiedDateTime"] {
        Value::Null => created,
        value => graph_time(value)?,
    };
    let body = message["body"]["content"].as_str().unwrap_or_default();
    let media_type = if message["body"]["contentType"] == "html" {
        "text/html"
    } else {
        "text/plain"
    };
    let object = format!("{container}/{id}");
    let revision = stable_id(
        REPRESENTATION,
        &[object.as_bytes(), &modified.to_be_bytes(), body.as_bytes()],
    );
    let mut source = json!({"authority":context.authority,"object_id":object,"revision":revision});
    if let Some(link) = message["webUrl"]
        .as_str()
        .filter(|link| link.starts_with("https://") && link.len() <= 8192)
    {
        source["url"] = link.into();
    }
    let metadata: ItemMetadata = serde_json::from_value(json!({"container_id":container,
        "path":object,"content_type":CONTENT_TYPE,"created_at_ms":created,"modified_at_ms":modified,"byte_length":body.len()}))
    .map_err(|_| ConnectorError::Response)?;
    let selection = context.scope.select(&metadata);
    let observation_id = stable_id(
        "teams-observation-v1",
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
    let supported = message["messageType"] == "message" && body.len() <= MAX_TEXT_BYTES;
    let (outcome, reason) = match selection {
        Selection::Excluded => ("skipped", selection.reason()),
        Selection::MissingMetadata => ("failed", selection.reason()),
        Selection::Selected if !supported => ("skipped", "unsupported"),
        Selection::Selected if body.trim().is_empty() => ("skipped", "empty"),
        Selection::Selected => ("fetched", "in_scope"),
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
        let mut change = json!({"type":kind,"change_id":stable_id("teams-change-v1", &[observation_id.as_bytes(),kind.as_bytes()]),
            "observation_id":observation_id,"source":source,"observed_at_ms":observed_at_ms});
        if kind == "upsert" {
            let author = message["from"]["user"]["displayName"]
                .as_str()
                .unwrap_or_default();
            change["permissions"] = json!({"state":"unavailable"});
            change["metadata"] = json!({"provider":"teams","channel":container,"message_id":id,
                "reply_to":message["replyToId"].as_str().unwrap_or_default(),"author":author});
            change["content"] = json!({"kind":"text","text":body,"media_type":media_type,
                "representation_version":REPRESENTATION});
        }
        events.push(change);
    }
    events
        .into_iter()
        .map(|event| serde_json::from_value(event).map_err(|_| ConnectorError::Response))
        .collect()
}
