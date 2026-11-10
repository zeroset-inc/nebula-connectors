//! Text reads and file events shared by the file storage adapters.

use std::future::Future;

use nebula_connector_sdk::{
    ClientError, ConnectorError, Context,
    client::ProviderFailureCode,
    connector::stable_id,
    protocol::{ConnectorEvent, ItemMetadata},
    scope::Selection,
};
use serde_json::{Value, json};

use crate::common::{Result, broker_read_bounded};

/// Files per page. Each file costs two events and up to `MAX_TEXT_BYTES` of
/// content, so two keep a page within the runner's 1 MiB batch.
pub(crate) const PAGE_SIZE: usize = 2;
/// Largest text file read; the catalogs cap downloads at this size.
pub(crate) const MAX_TEXT_BYTES: u64 = 192 << 10;
/// JSON escaping can grow text; larger encodings are skipped, not batched.
const MAX_ENCODED_TEXT: usize = 400 << 10;

/// The media type a text-like file is read as, by its type or extension.
pub(crate) fn text_type(mime_type: &str, name: &str) -> Option<&'static str> {
    let extension = name
        .rsplit_once('.')
        .map(|(_, extension)| extension.to_ascii_lowercase());
    match (mime_type, extension.as_deref()) {
        ("text/markdown", _) | (_, Some("md" | "markdown")) => Some("text/markdown"),
        ("text/html", _) | (_, Some("html" | "htm")) => Some("text/html"),
        ("text/plain" | "text/csv" | "text/tab-separated-values", _)
        | (_, Some("txt" | "csv" | "tsv")) => Some("text/plain"),
        _ => None,
    }
}

pub(crate) enum Fetched {
    Text(String),
    Skipped(&'static str),
    Failed(&'static str),
}

/// Reads a text file through the broker. An unreadable or oversized reply
/// skips the file, a refused token fails the run, and other refusals fail
/// only this file.
pub(crate) async fn fetch_text(
    context: &Context,
    operation: &str,
    arguments: Value,
    max_bytes: u64,
) -> Result<Fetched> {
    let body = match broker_read_bounded(context, operation, arguments, Some(max_bytes)).await {
        Ok(response) => response.body,
        Err(ClientError::Provider(failure)) => {
            return Ok(match (failure.code, failure.provider_status) {
                (ProviderFailureCode::ResponseLimit, _) => Fetched::Skipped("too_large"),
                (ProviderFailureCode::InvalidResponse, _) => Fetched::Skipped("unsupported"),
                (_, 401) => return Err(ConnectorError::Permission),
                (_, 403) => Fetched::Failed("access_denied"),
                (_, 404 | 409) => Fetched::Failed("unavailable"),
                _ => Fetched::Failed("fetch_failed"),
            });
        }
        Err(error) => return Err(error.into()),
    };
    let Value::String(text) = body else {
        return Err(ConnectorError::Response);
    };
    if text.trim().is_empty() {
        return Ok(Fetched::Skipped("empty"));
    }
    if serde_json::to_string(&text)
        .map_err(|_| ConnectorError::Response)?
        .len()
        > MAX_ENCODED_TEXT
    {
        return Ok(Fetched::Skipped("unsupported"));
    }
    Ok(Fetched::Text(text))
}

/// A file directly inside a selected folder.
pub(crate) struct File<'a> {
    pub(crate) provider: &'a str,
    pub(crate) representation: &'a str,
    pub(crate) container: &'a str,
    pub(crate) id: &'a str,
    pub(crate) name: &'a str,
    /// The provider's media type, when it reports one.
    pub(crate) mime_type: Option<&'a str>,
    /// What the provider changes only with the content, such as a content
    /// hash, so renames and moves keep the revision.
    pub(crate) content_tag: &'a str,
    pub(crate) size: Option<u64>,
    pub(crate) created_at_ms: Option<i64>,
    pub(crate) modified_at_ms: i64,
    pub(crate) url: Option<&'a str>,
}

impl File<'_> {
    pub(crate) fn media_type(&self) -> Option<&'static str> {
        text_type(self.mime_type.unwrap_or_default(), self.name)
    }
}

/// The file's observation and, when it is selected, its change. `fetch`
/// reads the content, and runs only for a selected text file within
/// `MAX_TEXT_BYTES`.
pub(crate) async fn file_events<F>(
    context: &Context,
    file: &File<'_>,
    observed_at_ms: i64,
    fetch: impl FnOnce(u64) -> F,
) -> Result<Vec<ConnectorEvent>>
where
    F: Future<Output = Result<Fetched>>,
{
    let media_type = file.media_type();
    let revision = stable_id(
        file.representation,
        &[file.id.as_bytes(), file.content_tag.as_bytes()],
    );
    let mut source = json!({"authority":context.authority,"object_id":file.id,"revision":revision});
    if let Some(link) = file
        .url
        .filter(|link| link.starts_with("https://") && link.len() <= 8192)
    {
        source["url"] = link.into();
    }
    let content_type = file
        .mime_type
        .or(media_type)
        .unwrap_or("application/octet-stream");
    let mut metadata = json!({"container_id":file.container,"path":format!("{}/{}", file.container, file.name),
        "content_type":content_type.chars().take(256).collect::<String>(),"modified_at_ms":file.modified_at_ms});
    if let Some(created) = file.created_at_ms {
        metadata["created_at_ms"] = created.into();
    }
    if let Some(size) = file.size {
        metadata["byte_length"] = size.into();
    }
    let metadata: ItemMetadata =
        serde_json::from_value(metadata).map_err(|_| ConnectorError::Response)?;
    let selection = context.scope.select(&metadata);
    let observation_id = stable_id(
        &format!("{}-observation-v1", file.provider),
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
    // Content is read only for selected files, so an exclusion never fetches.
    let fetched = match (selection, media_type) {
        (Selection::Selected, Some(_)) if file.size.is_some_and(|size| size > MAX_TEXT_BYTES) => {
            Fetched::Skipped("too_large")
        }
        (Selection::Selected, Some(_)) => {
            let eligibility = context
                .service
                .source_eligibility(&context.run, file.name, content_type, file.size)
                .await?;
            match eligibility.reason {
                Some(reason) => Fetched::Skipped(reason.as_str()),
                None => fetch(eligibility.max_bytes.min(MAX_TEXT_BYTES)).await?,
            }
        }
        (Selection::Selected, None) => Fetched::Skipped("unsupported"),
        (_, _) => Fetched::Skipped(selection.reason()),
    };
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
        let mut change = json!({"type":kind,"change_id":stable_id(&format!("{}-change-v1", file.provider), &[observation_id.as_bytes(),kind.as_bytes()]),
            "observation_id":observation_id,"source":source,"observed_at_ms":observed_at_ms});
        if let (Fetched::Text(text), Some(media_type)) = (fetched, media_type) {
            change["permissions"] = json!({"state":"unavailable"});
            change["metadata"] = json!({"provider":file.provider,"item_id":file.id,"name":file.name,"filename":file.name,"container_id":file.container});
            if let Some(mime_type) = file.mime_type {
                change["metadata"]["mime_type"] = mime_type.into();
            }
            change["content"] = json!({"kind":"text","text":text,"media_type":media_type,
                "representation_version":file.representation});
        }
        events.push(change);
    }
    events
        .into_iter()
        .map(|event| serde_json::from_value(event).map_err(|_| ConnectorError::Response))
        .collect()
}
