use std::{future::Future, time::Duration};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::time::Instant;

use crate::{
    Client, ClientError, Connector, ConnectorError, Context, Page, Run, ValidatedBatch,
    connector::stable_id,
    validation::{MAX_BATCH_BYTES, MAX_CURSOR_BYTES, encode_bounded},
};

const MAX_PAGES: usize = 10_000;
const MAX_RUN_TIME: Duration = Duration::from_secs(3600);
const LEASE_MARGIN: Duration = Duration::from_secs(1);

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("runner_configuration_invalid")]
    Configuration,
    #[error("runner_page_invalid")]
    Page,
    #[error("runner_cursor_version_mismatch")]
    CursorVersion,
    #[error("runner_lease_lost")]
    LeaseLost,
    #[error("runner_limit_reached")]
    Limit,
    #[error(transparent)]
    Client(#[from] ClientError),
    #[error(transparent)]
    Connector(#[from] ConnectorError),
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    version: u32,
    value: Value,
}

/// Execute one scan, retaining one page until its durable acceptance. Restart
/// with the same run ID to recover its checkpoint, or a new ID after lease expiry.
/// Dropping this future cancels the in-flight read and stops lease renewal.
pub async fn run(
    connector: &dyn Connector,
    service: Client,
    run_id: &str,
    scan_id: &str,
    complete_inventory: bool,
) -> Result<Run, Error> {
    let connection = service.connection().await?;
    let spec = connector.specification();
    if spec.provider != connection.provider
        || spec.cursor_version == 0
        || connection.state != "active"
        || complete_inventory && !spec.capabilities.complete_inventory
        || !complete_inventory && !spec.capabilities.incremental
        || connection.credential_mode == crate::client::CredentialMode::Managed
            && !spec.capabilities.managed
    {
        return Err(Error::Configuration);
    }
    encode_bounded(&spec.config_schema, MAX_CURSOR_BYTES).map_err(|_| Error::Configuration)?;
    let validator =
        jsonschema::validator_for(&spec.config_schema).map_err(|_| Error::Configuration)?;
    if !validator.is_valid(&connection.config) {
        return Err(Error::Configuration);
    }
    let scope = crate::scope::Filter::parse(
        &encode_bounded(&connection.scope, MAX_CURSOR_BYTES).map_err(|_| Error::Configuration)?,
        connection.recurring,
        &spec.capabilities.supported_filters,
    )
    .map_err(|_| Error::Configuration)?;
    let started = Instant::now();
    let run = service.begin(run_id, scan_id, complete_inventory).await?;
    if run.scope_revision != connection.scope_revision
        || run.credential_revision != connection.credential_revision
        || run.authorization_revision != connection.authorization_revision
    {
        return Err(Error::Configuration);
    }
    let cursor = decode_cursor(&run.cursor, spec.cursor_version)?;
    if run.state == "sealed" {
        return Ok(run);
    }
    let deadline = lease_deadline(started, run.lease_remaining_ms)?;
    let renewal = keep_lease(&service, &run, deadline, complete_inventory);
    let mut context = Context {
        service: service.clone(),
        run: run.clone(),
        config: connection.config,
        authority: connection.authority,
        scope,
        complete_inventory,
        credential_mode: connection.credential_mode,
    };
    let execute = async {
        connector.check(&context).await?;
        read_pages(
            connector,
            &mut context,
            cursor,
            spec.cursor_version,
            complete_inventory,
        )
        .await
    };
    tokio::select! {
        biased;
        result = execute => result,
        result = renewal => result,
        _ = tokio::time::sleep(MAX_RUN_TIME) => Err(Error::Limit),
    }
}

fn decode_cursor(value: &Value, version: u32) -> Result<Value, Error> {
    if value.as_object().is_some_and(|v| v.is_empty()) {
        return Ok(json!({}));
    }
    let cursor: Cursor = serde_json::from_value(value.clone()).map_err(|_| Error::CursorVersion)?;
    if cursor.version != version || !cursor.value.is_object() {
        return Err(Error::CursorVersion);
    }
    Ok(cursor.value)
}

async fn read_pages(
    connector: &dyn Connector,
    context: &mut Context,
    mut cursor: Value,
    version: u32,
    complete_inventory: bool,
) -> Result<Run, Error> {
    for _ in 0..MAX_PAGES {
        let page = connector.read(context, &cursor).await?;
        if !page.complete && page.cursor == cursor {
            return Err(Error::Page);
        }
        let batch = prepare_batch(context, &page, version, complete_inventory)?;
        let receipt = retry(|| context.service.accept(&context.run, &batch)).await?;
        let value: Value = serde_json::from_slice(batch.bytes()).map_err(|_| Error::Page)?;
        let expected: Vec<&str> = value["events"]
            .as_array()
            .ok_or(Error::Page)?
            .iter()
            .filter_map(|event| event.get("change_id").and_then(Value::as_str))
            .collect();
        if receipt.changes != expected {
            return Err(ClientError::Response.into());
        }
        context.run.next_sequence = context
            .run
            .next_sequence
            .checked_add(1)
            .ok_or(Error::Limit)?;
        context.run.cursor = json!({"version":version,"value":page.cursor});
        if page.complete {
            if !complete_inventory {
                retry(|| context.service.complete(&context.run)).await?;
            }
            context.run.state = "sealed".into();
            return Ok(context.run.clone());
        }
        cursor = page.cursor;
    }
    Err(Error::Limit)
}

fn prepare_batch(
    context: &Context,
    page: &Page,
    version: u32,
    complete_inventory: bool,
) -> Result<ValidatedBatch, Error> {
    if page.events.len() > super::client::ProtocolLimits::V1.max_batch_events - 2
        || !page.cursor.is_object()
    {
        return Err(Error::Page);
    }
    let cursor = Cursor {
        version,
        value: page.cursor.clone(),
    };
    encode_bounded(&cursor, MAX_CURSOR_BYTES).map_err(|_| Error::Page)?;
    let bytes = encode_bounded(&page.events, MAX_BATCH_BYTES).map_err(|_| Error::Page)?;
    let mut events: Vec<Value> = serde_json::from_slice(&bytes).map_err(|_| Error::Page)?;
    if events.iter().any(|event| {
        matches!(
            event["type"].as_str(),
            Some("checkpoint" | "snapshot_complete")
        )
    }) {
        return Err(Error::Page);
    }
    if page.complete && complete_inventory {
        events.push(json!({"type":"snapshot_complete","scan_id":context.run.scan_id,"scope_revision":context.run.scope_revision}));
    }
    events.push(json!({"type":"checkpoint","cursor":cursor}));
    let batch_id = stable_id(
        "batch_v1",
        &[
            context.service.connection_id().as_bytes(),
            context.run.id.as_bytes(),
            &context.run.fence.to_be_bytes(),
            &context.run.next_sequence.to_be_bytes(),
        ],
    );
    let bytes = encode_bounded(&json!({"protocol_version":1,"batch_id":batch_id,"fence":context.run.fence,"sequence":context.run.next_sequence,"events":events}), MAX_BATCH_BYTES).map_err(|_| Error::Page)?;
    ValidatedBatch::parse(&bytes).map_err(|_| Error::Page)
}

fn lease_deadline(started: Instant, remaining_ms: u64) -> Result<Instant, Error> {
    let remaining = Duration::from_millis(remaining_ms)
        .checked_sub(LEASE_MARGIN)
        .ok_or(Error::LeaseLost)?;
    let deadline = started + remaining;
    if deadline <= Instant::now() {
        return Err(Error::LeaseLost);
    }
    Ok(deadline)
}

async fn keep_lease(
    service: &Client,
    run: &Run,
    mut deadline: Instant,
    complete_inventory: bool,
) -> Result<Run, Error> {
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        tokio::time::sleep((remaining / 2).min(Duration::from_secs(30))).await;
        let started = Instant::now();
        match tokio::time::timeout_at(deadline, retry(|| service.renew(run))).await {
            Ok(Ok(lease)) => match lease_deadline(started, lease.lease_remaining_ms) {
                Ok(next) => deadline = next,
                Err(error) => return Err(error),
            },
            Ok(Err(ClientError::Rejected { status: 409, .. })) => {
                let recovered = tokio::time::timeout_at(
                    deadline,
                    service.begin(&run.id, &run.scan_id, complete_inventory),
                )
                .await;
                return match recovered {
                    Ok(Ok(current))
                        if current.state == "sealed"
                            && current.fence == run.fence
                            && current.scope_revision == run.scope_revision
                            && current.credential_revision == run.credential_revision
                            && current.authorization_revision == run.authorization_revision =>
                    {
                        Ok(current)
                    }
                    _ => Err(Error::LeaseLost),
                };
            }
            Ok(Err(error)) => return Err(Error::Client(error)),
            Err(_) => return Err(Error::LeaseLost),
        }
    }
}

async fn retry<T, F, Fut>(mut request: F) -> Result<T, ClientError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, ClientError>>,
{
    for attempt in 0..3 {
        match request().await {
            Ok(value) => return Ok(value),
            Err(error) => {
                let delay = match &error {
                    ClientError::Transport | ClientError::Capacity => {
                        Some(Duration::from_secs(1 << attempt))
                    }
                    ClientError::Rejected {
                        status,
                        retry_after,
                    } if *status == 429 || *status >= 500 => {
                        Some(retry_after.unwrap_or(Duration::from_secs(1 << attempt)))
                    }
                    _ => None,
                };
                if attempt == 2 || delay.is_none_or(|d| d > Duration::from_secs(30)) {
                    return Err(error);
                }
                tokio::time::sleep(delay.unwrap_or_default()).await;
            }
        }
    }
    unreachable!()
}
