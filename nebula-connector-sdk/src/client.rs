use std::{sync::Arc, time::Duration};

use reqwest::{
    Method, StatusCode, Url,
    header::{AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderValue},
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use tokio::sync::Semaphore;

use crate::{ValidatedBatch, validation::encode_bounded};

const MAX_RUN_RESPONSE: usize = 2 * crate::validation::MAX_CURSOR_BYTES + 8192;
const MAX_RECEIPT_RESPONSE: usize = 256 << 10;
const MAX_PROVIDER_RESPONSE: usize = (4 << 20) + 8192;

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("invalid_connector_configuration")]
    Configuration,
    #[error("connector_transport_failed")]
    Transport,
    #[error("invalid_connector_response")]
    Response,
    #[error("connector_request_rejected")]
    Rejected {
        status: u16,
        retry_after: Option<Duration>,
    },
    #[error("provider_request_failed")]
    Provider(ProviderFailure),
    #[error("connector_request_capacity_exhausted")]
    Capacity,
}

/// Longest one request to the service takes: the service's 65-second
/// deadline for a route that sends a provider request, and time to connect
/// and to answer.
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(75);

#[derive(Clone)]
pub struct Client {
    http: reqwest::Client,
    base: Url,
    connection: String,
    control_permits: Arc<Semaphore>,
    provider_permits: Arc<Semaphore>,
}

/// Fixed protocol-v1 transport/work ceilings; every run advertises the contract.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProtocolLimits {
    pub max_batch_events: usize,
    pub max_batch_bytes: usize,
    pub max_cursor_bytes: usize,
}
impl ProtocolLimits {
    pub const V1: Self = Self {
        max_batch_events: 100,
        max_batch_bytes: crate::validation::MAX_BATCH_BYTES,
        max_cursor_bytes: crate::validation::MAX_CURSOR_BYTES,
    };
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Run {
    pub limits: ProtocolLimits,
    pub id: String,
    pub scan_id: String,
    pub fence: u64,
    pub scope_revision: u64,
    pub credential_revision: u64,
    pub authorization_revision: u64,
    pub lease_until: String,
    pub lease_remaining_ms: u64,
    pub next_sequence: u64,
    pub cursor: Value,
    pub state: String,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CredentialMode {
    Managed,
    SelfManaged,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Connection {
    pub id: String,
    pub tenant_id: String,
    pub workspace_id: String,
    pub collection_id: String,
    pub provider: String,
    pub authority: String,
    pub config: Value,
    pub scope: Value,
    pub scope_revision: u64,
    pub credential_revision: u64,
    pub authorization_revision: u64,
    pub fence: u64,
    pub mode: String,
    pub credential_mode: CredentialMode,
    pub state: String,
    pub recurring: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Lease {
    pub lease_until: String,
    pub lease_remaining_ms: u64,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Receipt {
    pub batch_id: String,
    pub sequence: u64,
    pub fence: u64,
    pub sha256: String,
    pub changes: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderFailure {
    pub attempt_id: String,
    pub provider_status: u16,
    pub code: ProviderFailureCode,
    pub retryable: bool,
    pub retry_after_seconds: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderFailureCode {
    CredentialUnavailable,
    RequestInvalid,
    Revoked,
    TransportFailed,
    ResponseIncomplete,
    ResponseLimit,
    RedirectNotPermitted,
    ProviderRejected,
    InvalidResponse,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderResponse {
    pub attempt_id: String,
    pub status: u16,
    pub sha256: String,
    pub byte_length: u64,
    pub body: Value,
}

/// The revision of a provider file that a transfer stores.
#[derive(Clone, Copy, Debug)]
pub struct TransferFile<'a> {
    pub object_id: &'a str,
    pub revision: &'a str,
    pub filename: &'a str,
    pub content_type: &'a str,
    /// The upload an earlier transfer of this revision in this run left
    /// unverified. The service continues with it instead of reading the
    /// provider again.
    pub unverified_upload: Option<&'a str>,
}

/// An original file the service holds for this run. A file upsert refers to
/// it by `upload_id`, `sha256` and `byte_length`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Transfer {
    pub attempt_id: String,
    pub upload_id: String,
    pub sha256: String,
    pub byte_length: u64,
}

/// The service's answer to a transfer.
#[derive(Debug)]
pub enum Transferred {
    /// The service holds the original for this run.
    Held(Transfer),
    /// The engine has the upload and has not verified it within the request.
    /// A transfer that names it as `TransferFile::unverified_upload`, with a
    /// fresh attempt ID, continues from there.
    Unverified { upload_id: String },
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TransferReply {
    attempt_id: String,
    upload_id: String,
    sha256: Option<String>,
    byte_length: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceEligibility {
    pub eligible: bool,
    pub max_bytes: u64,
    pub reason: Option<SourceIneligible>,
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceIneligible {
    TooLarge,
    TableImportRequired,
}

impl SourceIneligible {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::TooLarge => "too_large",
            Self::TableImportRequired => "table_import_required",
        }
    }
}

impl Client {
    /// HTTPS is required except for loopback development or a local enterprise gateway.
    /// The bearer is a Nebula credential; provider tokens never enter this transport.
    pub fn new(
        endpoint: &str,
        workspace: &str,
        collection: &str,
        connection: &str,
        bearer: &str,
    ) -> Result<Self, ClientError> {
        let mut base = Url::parse(endpoint).map_err(|_| ClientError::Configuration)?;
        let loopback = base.host_str().is_some_and(|h| {
            h == "localhost"
                || h.trim_matches(['[', ']'])
                    .parse::<std::net::IpAddr>()
                    .is_ok_and(|a| a.is_loopback())
        });
        if base.scheme() != "https" && !(base.scheme() == "http" && loopback)
            || base.username() != ""
            || base.password().is_some()
            || base.query().is_some()
            || base.fragment().is_some()
            || base.cannot_be_a_base()
        {
            return Err(ClientError::Configuration);
        }
        for id in [workspace, collection, connection] {
            validate_id(id)?;
        }
        base.path_segments_mut()
            .map_err(|_| ClientError::Configuration)?
            .pop_if_empty()
            .extend([
                "v1",
                "workspaces",
                workspace,
                "collections",
                collection,
                "connections",
                connection,
            ]);
        if bearer.is_empty() || bearer.len() > 16384 {
            return Err(ClientError::Configuration);
        }
        let mut authorization = HeaderValue::from_str(&format!("Bearer {bearer}"))
            .map_err(|_| ClientError::Configuration)?;
        authorization.set_sensitive(true);
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, authorization);
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        let http = reqwest::Client::builder()
            .default_headers(headers)
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(10))
            .timeout(REQUEST_TIMEOUT)
            .build()
            .map_err(|_| ClientError::Configuration)?;
        Ok(Self {
            http,
            base,
            connection: connection.to_owned(),
            control_permits: Arc::new(Semaphore::new(4)),
            provider_permits: Arc::new(Semaphore::new(4)),
        })
    }

    pub fn connection_id(&self) -> &str {
        &self.connection
    }

    pub async fn connection(&self) -> Result<Connection, ClientError> {
        let connection: Connection = self.send(Method::GET, &[], None, 128 << 10).await?;
        if connection.id != self.connection
            || !connection.config.is_object()
            || !connection.scope.is_object()
        {
            return Err(ClientError::Response);
        }
        Ok(connection)
    }

    pub async fn begin(
        &self,
        run: &str,
        scan: &str,
        complete_inventory: bool,
    ) -> Result<Run, ClientError> {
        validate_id(run)?;
        validate_id(scan)?;
        let result: Run = self
            .send(
                Method::POST,
                &["runs"],
                Some(
                    serde_json::to_vec(
                        &json!({"id":run,"scan_id":scan,"complete_inventory":complete_inventory}),
                    )
                    .map_err(|_| ClientError::Configuration)?,
                ),
                MAX_RUN_RESPONSE,
            )
            .await?;
        if result.limits != ProtocolLimits::V1
            || result.id != run
            || result.scan_id != scan
            || result.fence == 0
            || !matches!(result.state.as_str(), "running" | "sealed")
            || result.lease_remaining_ms > 300_000
            || !result.cursor.is_object()
            || encode_bounded(&result.cursor, crate::validation::MAX_CURSOR_BYTES).is_err()
        {
            return Err(ClientError::Response);
        }
        Ok(result)
    }

    pub async fn renew(&self, run: &Run) -> Result<Lease, ClientError> {
        let lease: Lease = self
            .send(
                Method::POST,
                &["runs", &run.id, "renew"],
                Some(
                    serde_json::to_vec(&json!({"fence":run.fence}))
                        .map_err(|_| ClientError::Configuration)?,
                ),
                4096,
            )
            .await?;
        if lease.lease_remaining_ms == 0 || lease.lease_remaining_ms > 300_000 {
            return Err(ClientError::Response);
        }
        Ok(lease)
    }

    pub async fn complete(&self, run: &Run) -> Result<(), ClientError> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Completed {
            state: String,
        }
        let result: Completed = self
            .send(
                Method::POST,
                &["runs", &run.id, "complete"],
                Some(
                    serde_json::to_vec(
                        &json!({"fence":run.fence,"next_sequence":run.next_sequence}),
                    )
                    .map_err(|_| ClientError::Configuration)?,
                ),
                4096,
            )
            .await?;
        if result.state != "sealed" {
            return Err(ClientError::Response);
        }
        Ok(())
    }

    pub async fn accept(&self, run: &Run, batch: &ValidatedBatch) -> Result<Receipt, ClientError> {
        let wire = batch.batch();
        if wire.fence.get() != run.fence
            || u64::try_from(wire.sequence).ok() != Some(run.next_sequence)
        {
            return Err(ClientError::Configuration);
        }
        let receipt: Receipt = self
            .send(
                Method::POST,
                &["runs", &run.id, "batches"],
                Some(batch.bytes().to_vec()),
                MAX_RECEIPT_RESPONSE,
            )
            .await?;
        if receipt.fence != run.fence
            || receipt.sequence != run.next_sequence
            || receipt.batch_id != wire.batch_id.as_str()
            || !valid_digest(&receipt.sha256)
        {
            return Err(ClientError::Response);
        }
        Ok(receipt)
    }

    pub async fn receipt(&self, run: &Run, batch_id: &str) -> Result<Receipt, ClientError> {
        if batch_id.is_empty() || batch_id.chars().count() > 256 {
            return Err(ClientError::Configuration);
        }
        let receipt: Receipt = self
            .send(
                Method::POST,
                &["runs", &run.id, "receipts"],
                Some(
                    encode_bounded(&json!({"batch_id":batch_id}), 4096)
                        .map_err(|_| ClientError::Configuration)?,
                ),
                MAX_RECEIPT_RESPONSE,
            )
            .await?;
        if receipt.batch_id != batch_id
            || receipt.fence != run.fence
            || !valid_digest(&receipt.sha256)
        {
            return Err(ClientError::Response);
        }
        Ok(receipt)
    }

    /// Checks the admitted representation before downloading it. The decision
    /// reserves no capacity; actual bytes must still pass engine admission.
    pub async fn source_eligibility(
        &self,
        run: &Run,
        filename: &str,
        content_type: &str,
        size_bytes: Option<u64>,
    ) -> Result<SourceEligibility, ClientError> {
        let body = encode_bounded(
            &json!({
                "fence":run.fence,"filename":filename,
                "content_type":content_type,"size_bytes":size_bytes,
            }),
            65536,
        )
        .map_err(|_| ClientError::Configuration)?;
        let result: SourceEligibility = self
            .send(
                Method::POST,
                &["runs", &run.id, "source-eligibility"],
                Some(body),
                8192,
            )
            .await?;
        if result.max_bytes == 0 || result.eligible == result.reason.is_some() {
            return Err(ClientError::Response);
        }
        Ok(result)
    }

    /// Attempts are never silently retried. An uncertain result requires a fresh
    /// attempt ID; the service retains the original quota charge and audit record.
    pub async fn provider(
        &self,
        run: &Run,
        attempt: &str,
        operation: &str,
        arguments: Value,
    ) -> Result<ProviderResponse, ClientError> {
        self.provider_bounded(run, attempt, operation, arguments, None)
            .await
    }

    /// A caller may narrow the approved response limit, never expand it.
    pub async fn provider_bounded(
        &self,
        run: &Run,
        attempt: &str,
        operation: &str,
        arguments: Value,
        max_response_bytes: Option<u64>,
    ) -> Result<ProviderResponse, ClientError> {
        if max_response_bytes == Some(0) {
            return Err(ClientError::Configuration);
        }
        validate_id(attempt)?;
        #[derive(Serialize)]
        struct Request<'a> {
            fence: u64,
            id: &'a str,
            operation: &'a str,
            arguments: &'a Value,
            #[serde(skip_serializing_if = "Option::is_none")]
            max_response_bytes: Option<u64>,
        }
        let body = encode_bounded(
            &Request {
                fence: run.fence,
                id: attempt,
                operation,
                arguments: &arguments,
                max_response_bytes,
            },
            65536,
        )
        .map_err(|_| ClientError::Configuration)?;
        let response: ProviderResponse = self
            .send(
                Method::POST,
                &["runs", &run.id, "requests"],
                Some(body),
                MAX_PROVIDER_RESPONSE,
            )
            .await?;
        if response.attempt_id != attempt
            || !(200..300).contains(&response.status)
            || !valid_digest(&response.sha256)
            || response.byte_length > max_response_bytes.unwrap_or(2 << 20).min(2 << 20)
        {
            return Err(ClientError::Response);
        }
        Ok(response)
    }

    /// Has the broker read a provider file and store it as an original for
    /// this run, without the bytes passing through the runner. `max_bytes`
    /// narrows the size the operation approves, as the engine's eligibility
    /// answer does. A revision the connection already stored is answered
    /// with that original, without a read. The service answers within its
    /// usual request deadline: an upload the engine has not verified by then
    /// comes back as `Transferred::Unverified`. Like a provider attempt, a
    /// transfer is never silently retried: an uncertain result requires a
    /// fresh attempt ID.
    pub async fn transfer(
        &self,
        run: &Run,
        attempt: &str,
        operation: &str,
        arguments: Value,
        file: TransferFile<'_>,
        max_bytes: Option<u64>,
    ) -> Result<Transferred, ClientError> {
        if max_bytes == Some(0) {
            return Err(ClientError::Configuration);
        }
        validate_id(attempt)?;
        let mut request = json!({
            "fence":run.fence,"id":attempt,"operation":operation,"arguments":arguments,
            "object_id":file.object_id,"revision":file.revision,
            "filename":file.filename,"content_type":file.content_type,
        });
        if let Some(limit) = max_bytes {
            request["max_response_bytes"] = limit.into();
        }
        if let Some(upload) = file.unverified_upload {
            request["upload_id"] = upload.into();
        }
        let body = encode_bounded(&request, 65536).map_err(|_| ClientError::Configuration)?;
        let reply: TransferReply = self
            .send(
                Method::POST,
                &["runs", &run.id, "transfers"],
                Some(body),
                8192,
            )
            .await?;
        if reply.attempt_id != attempt || reply.upload_id.is_empty() || reply.upload_id.len() > 256
        {
            return Err(ClientError::Response);
        }
        match (reply.sha256, reply.byte_length) {
            (None, None) => Ok(Transferred::Unverified {
                upload_id: reply.upload_id,
            }),
            (Some(sha256), Some(byte_length))
                if valid_digest(&sha256) && max_bytes.is_none_or(|limit| byte_length <= limit) =>
            {
                Ok(Transferred::Held(Transfer {
                    attempt_id: reply.attempt_id,
                    upload_id: reply.upload_id,
                    sha256,
                    byte_length,
                }))
            }
            _ => Err(ClientError::Response),
        }
    }

    #[allow(
        clippy::disallowed_methods,
        reason = "Audited transport: bounded concurrency, timeout and response bytes; redirects disabled"
    )]
    async fn send<T: DeserializeOwned>(
        &self,
        method: Method,
        path: &[&str],
        body: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<T, ClientError> {
        let through_broker = matches!(path.last(), Some(&"requests" | &"transfers"));
        let permits = if through_broker {
            &self.provider_permits
        } else {
            &self.control_permits
        };
        let _permit = permits.try_acquire().map_err(|_| ClientError::Capacity)?;
        let mut url = self.base.clone();
        for id in path {
            validate_id(id)?;
        }
        url.path_segments_mut()
            .map_err(|_| ClientError::Configuration)?
            .extend(path);
        let mut request = self.http.request(method, url);
        if let Some(body) = body {
            request = request.body(body);
        }
        let mut response = request.send().await.map_err(|_| ClientError::Transport)?;
        let status = response.status();
        let retry_after = if status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error() {
            response
                .headers()
                .get("retry-after")
                .and_then(|h| h.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok())
                .map(|s| Duration::from_secs(s.min(86400)))
        } else {
            None
        };
        let limit = if status.is_success() { limit } else { 4096 };
        if response.content_length().is_some_and(|n| n > limit as u64) {
            return Err(if status.is_success() {
                ClientError::Response
            } else {
                ClientError::Rejected {
                    status: status.as_u16(),
                    retry_after,
                }
            });
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| ClientError::Transport)? {
            if chunk.len() > limit.saturating_sub(bytes.len()) {
                if status.is_success() {
                    return Err(ClientError::Response);
                }
                break;
            }
            bytes.extend_from_slice(&chunk);
        }
        if !status.is_success() {
            if through_broker
                && crate::validation::check_json(&bytes, 0).is_ok()
                && let Ok(failure) = serde_json::from_slice::<ProviderFailure>(&bytes)
                && failure.attempt_id.len() <= 256
                && failure.provider_status <= 599
                && failure.retry_after_seconds <= 86400
            {
                return Err(ClientError::Provider(failure));
            }
            return Err(ClientError::Rejected {
                status: status.as_u16(),
                retry_after,
            });
        }
        crate::validation::check_json(&bytes, 0).map_err(|_| ClientError::Response)?;
        serde_json::from_slice(&bytes).map_err(|_| ClientError::Response)
    }
}

fn validate_id(id: &str) -> Result<(), ClientError> {
    if id.is_empty()
        || id.len() > 1024
        || id == "."
        || id == ".."
        || id.chars().any(char::is_control)
    {
        Err(ClientError::Configuration)
    } else {
        Ok(())
    }
}
fn valid_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
