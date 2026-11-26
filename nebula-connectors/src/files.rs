//! Text reads and file events shared by the file storage adapters.

use std::future::Future;

use nebula_connector_sdk::{
    ClientError, ConnectorError, Context,
    client::{ProviderFailureCode, Transfer, TransferFile},
    connector::stable_id,
    protocol::{ConnectorEvent, ItemMetadata},
    scope::Selection,
};
use serde_json::{Value, json};

use crate::common::{Result, broker_read_bounded, broker_transfer};

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
    /// The file as the provider stores it, held by the service for this run.
    Original(Transfer),
    Skipped(&'static str),
    Failed(&'static str),
}

/// What an adapter reads of a selected file.
pub(crate) enum Read {
    /// The file's text, of at most this many bytes.
    Text(u64),
    /// The file itself, stored as an original under this source revision.
    Original { revision: String, max_bytes: u64 },
}

impl Read {
    pub(crate) fn max_bytes(&self) -> u64 {
        match self {
            Self::Text(max_bytes) | Self::Original { max_bytes, .. } => *max_bytes,
        }
    }
}

/// Has the broker store a file as an original. A file over the limit is
/// skipped, a refused token or a fenced run fails the run, a service that
/// offers no transfers leaves the file unsupported, and any other refusal
/// fails only this file.
pub(crate) async fn transfer_original(
    context: &Context,
    operation: &str,
    arguments: Value,
    file: TransferFile<'_>,
    max_bytes: u64,
) -> Result<Fetched> {
    Ok(
        match broker_transfer(context, operation, arguments, file, max_bytes).await {
            Ok(transfer) => Fetched::Original(transfer),
            Err(ClientError::Provider(failure)) => match (failure.code, failure.provider_status) {
                (ProviderFailureCode::ResponseLimit, _) => Fetched::Skipped("too_large"),
                (_, 401) => return Err(ConnectorError::Permission),
                (_, 403) => Fetched::Failed("access_denied"),
                (_, 404 | 409) => Fetched::Failed("unavailable"),
                _ => Fetched::Failed("fetch_failed"),
            },
            Err(ClientError::Rejected {
                status: 404 | 405 | 422,
                ..
            }) => Fetched::Skipped("unsupported"),
            Err(
                error @ ClientError::Rejected {
                    status: 401 | 403 | 409,
                    ..
                },
            ) => return Err(error.into()),
            Err(
                ClientError::Rejected { .. }
                | ClientError::Transport
                | ClientError::Capacity
                | ClientError::Response,
            ) => Fetched::Failed("fetch_failed"),
            Err(error) => return Err(error.into()),
        },
    )
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
    /// Names the revisions of files the adapter transfers as originals, such
    /// as "sharepoint-file-v1". Without it the adapter reads only text.
    pub(crate) originals: Option<&'a str>,
    pub(crate) container: &'a str,
    /// The folders between the container and the file, such as
    /// "Reports/2026"; empty for a file directly inside the container.
    pub(crate) folder: &'a str,
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

    fn path(&self) -> String {
        if self.folder.is_empty() {
            format!("{}/{}", self.container, self.name)
        } else {
            format!("{}/{}/{}", self.container, self.folder, self.name)
        }
    }
}

/// The file's observation and, when it is selected, its change. `fetch`
/// reads the content, and runs only for a selected file the engine accepts:
/// a text file within `MAX_TEXT_BYTES` as text, and any other file as an
/// original when the adapter transfers originals.
pub(crate) async fn file_events<F>(
    context: &Context,
    file: &File<'_>,
    observed_at_ms: i64,
    fetch: impl FnOnce(Read) -> F,
) -> Result<Vec<ConnectorEvent>>
where
    F: Future<Output = Result<Fetched>>,
{
    let media_type = file.media_type();
    let reads_text = media_type.is_some() && file.size.is_none_or(|size| size <= MAX_TEXT_BYTES);
    let representation = match file.originals {
        Some(originals) if !reads_text => originals,
        _ => file.representation,
    };
    let revision = stable_id(
        representation,
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
    let mut metadata = json!({"container_id":file.container,"path":file.path(),
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
    let fetched = if selection != Selection::Selected {
        Fetched::Skipped(selection.reason())
    } else if !reads_text && file.originals.is_none() {
        Fetched::Skipped(if media_type.is_some() {
            "too_large"
        } else {
            "unsupported"
        })
    } else {
        let eligibility = context
            .service
            .source_eligibility(&context.run, file.name, content_type, file.size)
            .await?;
        match eligibility.reason {
            Some(reason) => Fetched::Skipped(reason.as_str()),
            None if reads_text => {
                fetch(Read::Text(eligibility.max_bytes.min(MAX_TEXT_BYTES))).await?
            }
            None => {
                fetch(Read::Original {
                    revision: revision.clone(),
                    max_bytes: eligibility.max_bytes,
                })
                .await?
            }
        }
    };
    let (outcome, reason) = match (&fetched, selection) {
        (_, Selection::Excluded) => ("skipped", selection.reason()),
        (_, Selection::MissingMetadata) => ("failed", selection.reason()),
        (Fetched::Text(_) | Fetched::Original(_), _) => ("fetched", "in_scope"),
        (Fetched::Skipped(reason), _) => ("skipped", *reason),
        (Fetched::Failed(reason), _) => ("failed", *reason),
    };
    let mut events = vec![
        json!({"type":"item_observed","observation_id":observation_id,
        "scan_id":context.run.scan_id,"source":source,"observed_at_ms":observed_at_ms,
        "metadata":metadata,"selection":selection.selection(),"outcome":outcome,"reason":reason}),
    ];
    if let Fetched::Original(transfer) = &fetched {
        events[0]["sha256"] = transfer.sha256.as_str().into();
    }
    if selection == Selection::Selected {
        let kind = if outcome == "fetched" {
            "upsert"
        } else {
            "presence"
        };
        let mut change = json!({"type":kind,"change_id":stable_id(&format!("{}-change-v1", file.provider), &[observation_id.as_bytes(),kind.as_bytes()]),
            "observation_id":observation_id,"source":source,"observed_at_ms":observed_at_ms});
        let content = match (fetched, media_type) {
            (Fetched::Text(text), Some(media_type)) => {
                Some(json!({"kind":"text","text":text,"media_type":media_type,
                "representation_version":file.representation}))
            }
            (Fetched::Original(transfer), _) => {
                Some(json!({"kind":"file","upload_id":transfer.upload_id,
                "sha256":transfer.sha256,"byte_length":transfer.byte_length,"filename":file.name,
                "media_type":content_type.chars().take(256).collect::<String>(),"representation":"original"}))
            }
            _ => None,
        };
        if let Some(content) = content {
            change["permissions"] = json!({"state":"unavailable"});
            change["metadata"] = json!({"provider":file.provider,"item_id":file.id,"name":file.name,"filename":file.name,"container_id":file.container});
            if let Some(mime_type) = file.mime_type {
                change["metadata"]["mime_type"] = mime_type.into();
            }
            change["content"] = content;
        }
        events.push(change);
    }
    events
        .into_iter()
        .map(|event| serde_json::from_value(event).map_err(|_| ConnectorError::Response))
        .collect()
}
