use std::{
    collections::HashSet,
    future::Future,
    pin::Pin,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use nebula_connector_sdk::{
    ClientError, Connector, ConnectorError, Context, Page, Specification,
    client::CredentialMode,
    connector::{Capabilities, Resource, ResourcePage, stable_id},
    protocol::{ConnectorEvent, ItemMetadata},
    scope::Selection,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use uuid::Uuid;

const PAGE_SIZE: usize = 15;
const MAX_MESSAGE_BYTES: usize = 48 << 10;
const REPRESENTATION: &str = "slack-message-v1";
type Result<T> = std::result::Result<T, ConnectorError>;
type Response<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;

/// Managed Slack channel inventories. Each message or reply has its own source.
/// Files, incremental delivery and permission mirroring require separate capabilities.
pub struct Slack;

impl Connector for Slack {
    fn specification(&self) -> Specification {
        Specification {
            provider: "slack".into(),
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
            let response = request(context, "auth.test", json!({})).await?;
            if response["team_id"].as_str() != Some(context.authority.as_str()) {
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
            #[derive(Default, Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Discovery {
                #[serde(default)]
                channel: usize,
            }
            let position: Discovery = serde_json::from_value(cursor.cloned().unwrap_or(json!({})))
                .map_err(|_| ConnectorError::CursorExpired)?;
            let channels = context.scope.containers();
            let channel = channels
                .get(position.channel)
                .ok_or(ConnectorError::CursorExpired)?;
            let response =
                request(context, "conversations.info", json!({"channel":channel})).await?;
            let info = &response["channel"];
            if info["id"].as_str() != Some(channel.as_str()) {
                return Err(ConnectorError::Response);
            }
            let name = info["name"]
                .as_str()
                .filter(|name| !name.is_empty() && name.len() <= 1024)
                .ok_or(ConnectorError::Response)?;
            Ok(ResourcePage {
                resources: vec![Resource {
                    id: channel.clone(),
                    name: name.into(),
                    kind: "channel".into(),
                    parent_id: Some(context.authority.clone()),
                }],
                next: (position.channel + 1 < channels.len())
                    .then(|| json!({"channel":position.channel + 1})),
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

fn validate_context(context: &Context) -> Result<()> {
    if context.credential_mode != CredentialMode::Managed
        || !context.complete_inventory
        || context.config != json!({})
        || !identifier(&context.authority, b"T")
        || context
            .scope
            .containers()
            .iter()
            .any(|id| !identifier(id, b"CG"))
    {
        return Err(ConnectorError::Configuration);
    }
    Ok(())
}

fn identifier(value: &str, prefixes: &[u8]) -> bool {
    (2..=64).contains(&value.len())
        && prefixes.contains(&value.as_bytes()[0])
        && value.bytes().all(|byte| byte.is_ascii_alphanumeric())
}

async fn request(context: &Context, operation: &str, arguments: Value) -> Result<Value> {
    let response = broker_read(context, operation, arguments).await?;
    if response.body["ok"] == true {
        return Ok(response.body);
    }
    Err(match response.body["error"].as_str() {
        Some(
            "invalid_auth"
            | "not_authed"
            | "token_revoked"
            | "token_expired"
            | "account_inactive"
            | "missing_scope"
            | "not_allowed_token_type"
            | "access_denied"
            | "not_in_channel"
            | "channel_not_found",
        ) => ConnectorError::Permission,
        Some("invalid_cursor") => ConnectorError::CursorExpired,
        _ => ConnectorError::Response,
    })
}

async fn broker_read(
    context: &Context,
    operation: &str,
    arguments: Value,
) -> std::result::Result<nebula_connector_sdk::client::ProviderResponse, ClientError> {
    for attempt in 0..3 {
        let error = match context
            .service
            .provider(
                &context.run,
                &Uuid::new_v4().to_string(),
                operation,
                arguments.clone(),
            )
            .await
        {
            Ok(response) => return Ok(response),
            Err(error) => error,
        };
        let fallback = Duration::from_secs(1 << attempt);
        let delay = match &error {
            ClientError::Rejected {
                status,
                retry_after,
            } if *status == 429 || *status >= 500 => Some(retry_after.unwrap_or(fallback)),
            ClientError::Provider(failure) if failure.retryable => {
                Some(if failure.retry_after_seconds == 0 {
                    fallback
                } else {
                    Duration::from_secs(failure.retry_after_seconds)
                })
            }
            ClientError::Transport | ClientError::Capacity => Some(fallback),
            _ => None,
        };
        if attempt == 2 || delay.is_none_or(|delay| delay > Duration::from_secs(120)) {
            return Err(error);
        }
        tokio::time::sleep(delay.unwrap_or_default()).await;
    }
    unreachable!()
}

#[derive(Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
struct Cursor {
    channel: usize,
    history: String,
    history_done: bool,
    threads: Vec<String>,
    reply: String,
}

impl Cursor {
    fn parse(value: &Value, channels: usize) -> Result<Self> {
        let cursor: Self =
            serde_json::from_value(value.clone()).map_err(|_| ConnectorError::CursorExpired)?;
        let unique: HashSet<_> = cursor.threads.iter().collect();
        if cursor.channel > channels
            || cursor.history.len() > 4096
            || cursor.reply.len() > 4096
            || cursor.threads.len() > PAGE_SIZE
            || unique.len() != cursor.threads.len()
            || cursor.threads.iter().any(|ts| timestamp_ms(ts).is_err())
            || cursor.threads.is_empty() && !cursor.reply.is_empty()
            || cursor.channel == channels
                && (!cursor.history.is_empty()
                    || !cursor.reply.is_empty()
                    || !cursor.threads.is_empty())
        {
            return Err(ConnectorError::CursorExpired);
        }
        Ok(cursor)
    }

    fn advance(&mut self) {
        if self.threads.is_empty() && self.history_done {
            self.channel += 1;
            self.history.clear();
            self.history_done = false;
        }
    }
}

async fn read(context: &Context, value: &Value) -> Result<Page> {
    validate_context(context)?;
    let channels = context.scope.containers();
    let mut cursor = Cursor::parse(value, channels.len())?;
    let Some(channel) = channels.get(cursor.channel) else {
        return page(cursor, vec![], true);
    };
    let thread = cursor.threads.first().cloned();
    let previous = if thread.is_some() {
        &cursor.reply
    } else {
        &cursor.history
    };
    let mut arguments = json!({"channel":channel,"limit":PAGE_SIZE.to_string()});
    if !previous.is_empty() {
        arguments["cursor"] = previous.clone().into();
    }
    let operation = if let Some(thread) = &thread {
        arguments["ts"] = thread.clone().into();
        "conversations.replies"
    } else {
        "conversations.history"
    };
    let response = request(context, operation, arguments).await?;
    if response
        .get("is_limited")
        .is_some_and(|value| value != false)
    {
        return Err(ConnectorError::IncompleteInventory);
    }
    let messages = response["messages"]
        .as_array()
        .ok_or(ConnectorError::Response)?;
    if messages.len() > PAGE_SIZE {
        return Err(ConnectorError::Response);
    }
    let next = next_cursor(&response, previous)?;
    let observed_at_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|time| i64::try_from(time.as_millis()).ok())
        .filter(|time| *time <= 9_007_199_254_740_991)
        .ok_or(ConnectorError::Response)?;
    let mut events = Vec::with_capacity(messages.len() * 2);
    let mut identities = HashSet::with_capacity(messages.len());
    for message in messages {
        let ts = message["ts"].as_str().ok_or(ConnectorError::Response)?;
        timestamp_ms(ts)?;
        if !identities.insert(ts) {
            return Err(ConnectorError::Response);
        }
        if let Some(thread) = &thread {
            if ts == thread {
                continue;
            }
            if message["thread_ts"].as_str() != Some(thread.as_str()) {
                return Err(ConnectorError::Response);
            }
        } else {
            let replies = match message.get("reply_count") {
                None => 0,
                Some(value) => value.as_u64().ok_or(ConnectorError::Response)?,
            };
            if replies > 0
                && message
                    .get("thread_ts")
                    .is_none_or(|value| value.as_str() == Some(ts))
            {
                cursor.threads.push(ts.into());
            }
        }
        events.extend(message_events(context, channel, message, observed_at_ms)?);
    }
    if thread.is_some() {
        cursor.reply = next;
        if cursor.reply.is_empty() {
            cursor.threads.remove(0);
        }
    } else {
        cursor.history_done = next.is_empty();
        cursor.history = next;
    }
    cursor.advance();
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

fn next_cursor(response: &Value, previous: &str) -> Result<String> {
    let next = match response.get("response_metadata") {
        None => "",
        Some(metadata) if metadata.is_object() => match metadata.get("next_cursor") {
            None => "",
            Some(Value::String(value)) => value,
            _ => return Err(ConnectorError::Response),
        },
        _ => return Err(ConnectorError::Response),
    };
    let has_more = match response.get("has_more") {
        None => false,
        Some(Value::Bool(value)) => *value,
        _ => return Err(ConnectorError::Response),
    };
    if next.len() > 4096 || !next.is_empty() && next == previous || has_more && next.is_empty() {
        return Err(ConnectorError::IncompleteInventory);
    }
    Ok(next.into())
}

fn timestamp_ms(value: &str) -> Result<i64> {
    let (seconds, fraction) = value.split_once('.').ok_or(ConnectorError::Response)?;
    if seconds.is_empty()
        || seconds.len() > 10
        || fraction.len() != 6
        || !seconds
            .bytes()
            .chain(fraction.bytes())
            .all(|byte| byte.is_ascii_digit())
    {
        return Err(ConnectorError::Response);
    }
    let seconds: i64 = seconds.parse().map_err(|_| ConnectorError::Response)?;
    let fraction: i64 = fraction[..3]
        .parse()
        .map_err(|_| ConnectorError::Response)?;
    Ok(seconds * 1000 + fraction)
}

fn message_events(
    context: &Context,
    channel: &str,
    message: &Value,
    observed_at_ms: i64,
) -> Result<Vec<ConnectorEvent>> {
    let ts = message["ts"].as_str().ok_or(ConnectorError::Response)?;
    let created = timestamp_ms(ts)?;
    let thread = match message.get("thread_ts") {
        None => ts,
        Some(value) => value.as_str().ok_or(ConnectorError::Response)?,
    };
    timestamp_ms(thread)?;
    let modified = match message.get("edited") {
        None => created,
        Some(edited) => timestamp_ms(edited["ts"].as_str().ok_or(ConnectorError::Response)?)?,
    };
    let mut representation = json!({"channel":channel,"thread_ts":thread});
    for key in [
        "type",
        "subtype",
        "text",
        "user",
        "bot_id",
        "ts",
        "edited",
        "blocks",
        "attachments",
        "files",
    ] {
        if let Some(value) = message.get(key) {
            representation[key] = value.clone();
        }
    }
    let bytes = serde_json::to_vec(&representation).map_err(|_| ConnectorError::Response)?;
    let revision = stable_id(REPRESENTATION, &[&bytes]);
    let object = format!("{channel}:{ts}");
    let source = json!({"authority":context.authority,"object_id":object,"revision":revision,
        "url":format!("slack://channel?team={}&id={channel}&message={ts}",context.authority)});
    let metadata: ItemMetadata = serde_json::from_value(json!({"container_id":channel,
        "path":format!("{channel}/{ts}"),"content_type":"application/vnd.slack.message+json",
        "created_at_ms":created,"modified_at_ms":modified,"byte_length":bytes.len()}))
    .map_err(|_| ConnectorError::Response)?;
    let selection = context.scope.select(&metadata);
    let observation_id = stable_id(
        "slack-observation-v1",
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
    let supported = message["type"] == "message"
        && message
            .get("files")
            .is_none_or(|files| files.as_array().is_some_and(Vec::is_empty))
        && bytes.len() <= MAX_MESSAGE_BYTES;
    let empty = message["text"]
        .as_str()
        .is_none_or(|text| text.trim().is_empty())
        && ["blocks", "attachments"].iter().all(|key| {
            message
                .get(key)
                .is_none_or(|value| value.as_array().is_some_and(Vec::is_empty))
        });
    let (outcome, reason) = match selection {
        Selection::Excluded => ("skipped", selection.reason()),
        Selection::MissingMetadata => ("failed", selection.reason()),
        Selection::Selected if !supported => ("skipped", "unsupported"),
        Selection::Selected if empty => ("skipped", "empty"),
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
        let mut change = json!({"type":kind,"change_id":stable_id("slack-change-v1", &[observation_id.as_bytes(),kind.as_bytes()]),
            "observation_id":observation_id,"source":source,"observed_at_ms":observed_at_ms});
        if kind == "upsert" {
            change["permissions"] = json!({"state":"unavailable"});
            change["metadata"] =
                json!({"provider":"slack","channel_id":channel,"message_ts":ts,"thread_ts":thread});
            change["content"] = json!({"kind":"structured","value":representation,
                "media_type":"application/json","representation_version":REPRESENTATION});
        }
        events.push(change);
    }
    events
        .into_iter()
        .map(|event| serde_json::from_value(event).map_err(|_| ConnectorError::Response))
        .collect()
}
