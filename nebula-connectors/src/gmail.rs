use std::collections::HashSet;

use base64::{
    Engine, alphabet,
    engine::{DecodePaddingMode, GeneralPurpose, GeneralPurposeConfig},
};
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
    google::validate_context,
};

/// Messages per page. Each costs two events and up to `MAX_TEXT_BYTES` of
/// content, so four keep a page within the runner's 1 MiB batch.
const PAGE_SIZE: usize = 4;
const MAX_TEXT_BYTES: usize = 96 << 10;
/// JSON escaping can grow text; larger encodings are skipped, not batched.
const MAX_ENCODED_TEXT: usize = 200 << 10;
/// Attachments and nesting deeper than this are not searched for body text.
const MAX_PART_DEPTH: usize = 8;
const REPRESENTATION: &str = "gmail-message-v1";
const CONTENT_TYPE: &str = "message/rfc822";

/// Gmail encodes part bodies as base64url, with or without padding.
const BODY: GeneralPurpose = GeneralPurpose::new(
    &alphabet::URL_SAFE,
    GeneralPurposeConfig::new().with_decode_padding_mode(DecodePaddingMode::Indifferent),
);

/// Managed Gmail label inventories: every message carrying a selected label.
/// A message's text is its plain body, or its HTML body when it has no plain
/// one, behind its subject, sender, recipients and date. Attachments are not
/// read until file transfer exists.
pub struct Gmail;

impl Connector for Gmail {
    fn specification(&self) -> Specification {
        Specification {
            provider: "gmail".into(),
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
            let profile = request(context, "users.getProfile", json!({})).await?;
            profile["emailAddress"]
                .as_str()
                .filter(|address| !address.is_empty())
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
            #[derive(Default, serde::Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Discovery {
                #[serde(default)]
                label: usize,
            }
            let position: Discovery = serde_json::from_value(cursor.cloned().unwrap_or(json!({})))
                .map_err(|_| ConnectorError::CursorExpired)?;
            let labels = context.scope.containers();
            let label = labels
                .get(position.label)
                .ok_or(ConnectorError::CursorExpired)?;
            let info = request(context, "users.labels.get", json!({"label":label})).await?;
            if info["id"].as_str() != Some(label.as_str()) {
                return Err(ConnectorError::Response);
            }
            let name = info["name"]
                .as_str()
                .filter(|name| !name.is_empty() && name.len() <= 1024)
                .ok_or(ConnectorError::Response)?;
            Ok(ResourcePage {
                resources: vec![Resource {
                    id: label.clone(),
                    name: name.into(),
                    kind: "label".into(),
                    parent_id: None,
                }],
                next: (position.label + 1 < labels.len())
                    .then(|| json!({"label":position.label + 1})),
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

/// Gmail message IDs are hexadecimal.
fn message_id(value: &str) -> bool {
    (1..=64).contains(&value.len()) && value.bytes().all(|byte| byte.is_ascii_alphanumeric())
}

async fn read(context: &Context, value: &Value) -> Result<Page> {
    validate_context(context)?;
    let labels = context.scope.containers();
    let mut cursor = Cursor::parse(value, labels.len())?;
    let Some(label) = labels.get(cursor.container) else {
        return page(cursor, vec![], true);
    };
    let mut arguments = json!({"label":label,"maxResults":PAGE_SIZE.to_string()});
    if !cursor.page.is_empty() {
        arguments["pageToken"] = cursor.page.clone().into();
    }
    let listing = request(context, "users.messages.list", arguments).await?;
    let messages = match listing.get("messages") {
        None => vec![],
        Some(Value::Array(messages)) => messages.clone(),
        _ => return Err(ConnectorError::Response),
    };
    if messages.len() > PAGE_SIZE {
        return Err(ConnectorError::Response);
    }
    let observed_at_ms = observed_now()?;
    let mut events = Vec::with_capacity(messages.len() * 2);
    let mut identities = HashSet::with_capacity(messages.len());
    for message in &messages {
        let id = message["id"]
            .as_str()
            .filter(|id| message_id(id))
            .ok_or(ConnectorError::Response)?;
        if !identities.insert(id.to_owned()) {
            return Err(ConnectorError::Response);
        }
        events.extend(message_events(context, label, id, observed_at_ms).await?);
    }
    cursor.advance(listing.get("nextPageToken"))?;
    let complete = cursor.container == labels.len();
    page(cursor, events, complete)
}

fn page(cursor: Cursor, events: Vec<ConnectorEvent>, complete: bool) -> Result<Page> {
    Ok(Page {
        cursor: serde_json::to_value(cursor).map_err(|_| ConnectorError::Response)?,
        events,
        complete,
    })
}

/// A message's body text, found in its first plain and first HTML parts that
/// are not attachments.
#[derive(Default)]
struct Bodies {
    plain: Option<String>,
    html: Option<String>,
}

fn collect_bodies(part: &Value, depth: usize, bodies: &mut Bodies) -> Result<()> {
    if depth > MAX_PART_DEPTH
        || part["filename"]
            .as_str()
            .is_some_and(|name| !name.is_empty())
    {
        return Ok(());
    }
    let slot = match part["mimeType"].as_str() {
        Some("text/plain") => Some(&mut bodies.plain),
        Some("text/html") => Some(&mut bodies.html),
        _ => None,
    };
    if let (Some(slot), Some(data)) = (slot, part["body"]["data"].as_str())
        && slot.is_none()
    {
        let bytes = BODY.decode(data).map_err(|_| ConnectorError::Response)?;
        *slot = String::from_utf8(bytes).ok();
    }
    if let Some(parts) = part["parts"].as_array() {
        for child in parts {
            collect_bodies(child, depth + 1, bodies)?;
        }
    }
    Ok(())
}

fn header<'a>(payload: &'a Value, name: &str) -> &'a str {
    payload["headers"]
        .as_array()
        .and_then(|headers| {
            headers.iter().find(|header| {
                header["name"]
                    .as_str()
                    .is_some_and(|found| found.eq_ignore_ascii_case(name))
            })
        })
        .and_then(|header| header["value"].as_str())
        .unwrap_or_default()
}

/// What reading a selected message produced: its text, or why it has none.
enum Fetched {
    Text {
        text: String,
        media_type: &'static str,
    },
    Skipped(&'static str),
    Failed(&'static str),
}

async fn message_events(
    context: &Context,
    label: &str,
    id: &str,
    observed_at_ms: i64,
) -> Result<Vec<ConnectorEvent>> {
    let message = match broker_read(
        context,
        "users.messages.get",
        json!({"message":id,"format":"full"}),
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
    let revision = stable_id(REPRESENTATION, &[id.as_bytes()]);
    let source = json!({"authority":context.authority,"object_id":id,"revision":revision,
        "url":format!("https://mail.google.com/mail/#all/{id}")});
    let mut metadata =
        json!({"container_id":label,"path":format!("{label}/{id}"),"content_type":CONTENT_TYPE});
    let mut subject = String::new();
    let fetched = match message {
        Err(fetched) => fetched,
        Ok(message) => {
            if message["id"].as_str() != Some(id) {
                return Err(ConnectorError::Response);
            }
            let sent: i64 = message["internalDate"]
                .as_str()
                .and_then(|date| date.parse().ok())
                .filter(|date| (0..=9_007_199_254_740_991).contains(date))
                .ok_or(ConnectorError::Response)?;
            metadata["created_at_ms"] = sent.into();
            metadata["modified_at_ms"] = sent.into();
            if let Some(size) = message["sizeEstimate"].as_u64() {
                metadata["byte_length"] = size.into();
            }
            let payload = &message["payload"];
            subject = header(payload, "Subject").chars().take(1024).collect();
            let mut bodies = Bodies::default();
            collect_bodies(payload, 0, &mut bodies)?;
            let (body, media_type) = match (bodies.plain, bodies.html) {
                (Some(plain), _) => (plain, "text/plain"),
                (None, Some(html)) => (html, "text/html"),
                (None, None) => (String::new(), "text/plain"),
            };
            let text = if media_type == "text/plain" {
                format!(
                    "Subject: {subject}\nFrom: {}\nTo: {}\nDate: {}\n\n{body}",
                    header(payload, "From"),
                    header(payload, "To"),
                    header(payload, "Date")
                )
            } else {
                body.clone()
            };
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
                Fetched::Text { text, media_type }
            }
        }
    };
    let metadata: ItemMetadata =
        serde_json::from_value(metadata).map_err(|_| ConnectorError::Response)?;
    let selection = context.scope.select(&metadata);
    let observation_id = stable_id(
        "gmail-observation-v1",
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
        (Fetched::Text { .. }, _) => ("fetched", "in_scope"),
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
        let mut change = json!({"type":kind,"change_id":stable_id("gmail-change-v1", &[observation_id.as_bytes(),kind.as_bytes()]),
            "observation_id":observation_id,"source":source,"observed_at_ms":observed_at_ms});
        if let Fetched::Text { text, media_type } = fetched {
            change["permissions"] = json!({"state":"unavailable"});
            change["metadata"] =
                json!({"provider":"gmail","message_id":id,"label_id":label,"subject":subject});
            change["content"] = json!({"kind":"text","text":text,"media_type":media_type,
                "representation_version":REPRESENTATION});
        }
        events.push(change);
    }
    events
        .into_iter()
        .map(|event| serde_json::from_value(event).map_err(|_| ConnectorError::Response))
        .collect()
}
