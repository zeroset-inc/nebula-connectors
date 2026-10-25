use std::collections::BTreeMap;

use base64::{Engine, engine::general_purpose};
use hmac::{Hmac, Mac};
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header, jwk::JwkSet};
use nebula_connector_sdk::{connector::stable_id, validation::decode_control};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::Sha256;
use zeroize::Zeroize;

pub const MAX_CALLBACK_BYTES: usize = 512 << 10;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Provider {
    Gmail,
    GoogleDrive,
    Slack,
    Notion,
    Outlook,
    #[serde(rename = "onedrive")]
    OneDrive,
    #[serde(rename = "sharepoint")]
    SharePoint,
    Teams,
}

impl Provider {
    pub const ALL: [Self; 8] = [
        Self::Gmail,
        Self::GoogleDrive,
        Self::Slack,
        Self::Notion,
        Self::Outlook,
        Self::OneDrive,
        Self::SharePoint,
        Self::Teams,
    ];
}

#[derive(Deserialize)]
#[serde(transparent)]
pub struct Secret(String);

impl Drop for Secret {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Context {
    pub version: u32,
    pub attempt_id: String,
    pub tenant_id: String,
    pub connection_id: String,
    pub subscription_id: String,
    pub scope_revision: u64,
    pub credential_revision: u64,
}

/// Supplied only by the authenticated broker, never by the public callback body.
#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Verification {
    Slack {
        signing_secret: Secret,
        team_id: String,
        app_id: String,
    },
    Notion {
        verification_token: Secret,
        workspace_id: String,
        subscription_id: String,
        bot_id: String,
    },
    Drive {
        channel_token: Secret,
        channel_id: String,
        resource_id: String,
    },
    Graph {
        client_state: Secret,
        subscription_id: String,
        tenant_id: String,
    },
    Gmail {
        audience: String,
        email: String,
        mailbox: String,
        subscription: String,
        jwks: JwkSet,
    },
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub context: Context,
    pub verification: Verification,
    pub verifying: bool,
    pub headers: BTreeMap<String, String>,
    pub query: BTreeMap<String, String>,
    pub body_base64: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Response {
    pub context: Context,
    pub outcome: Outcome,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Outcome {
    Challenge { value: String },
    Notifications { events: Vec<Notification> },
    Ignore,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Notification {
    pub event_id: String,
    pub kind: Change,
    pub container_id: String,
    pub object_id: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Change {
    Changed,
    Deleted,
    Reconcile,
    Revoked,
    Reauthorize,
    Missed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Invalid,
    Unauthorized,
    Unsupported,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Invalid => "invalid_callback",
            Self::Unauthorized => "callback_not_verified",
            Self::Unsupported => "callback_not_supported",
        })
    }
}
impl std::error::Error for Error {}

pub fn verify(provider: Provider, request: Request, now: u64) -> Result<Response, Error> {
    let context = &request.context;
    if context.version != 1
        || context.scope_revision == 0
        || context.credential_revision == 0
        || [
            &context.attempt_id,
            &context.tenant_id,
            &context.connection_id,
            &context.subscription_id,
        ]
        .into_iter()
        .any(|s| !valid_id(s))
        || request.headers.len() > 32
        || request.query.len() > 16
        || request.headers.iter().any(|(k, v)| {
            k.is_empty()
                || k.len() > 128
                || k != &k.to_ascii_lowercase()
                || !k
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
                || v.len() > 16384
                || v.contains(['\r', '\n'])
        })
        || request
            .query
            .iter()
            .any(|(k, v)| k.len() > 128 || v.len() > 4096)
        || request.body_base64.len() > MAX_CALLBACK_BYTES.div_ceil(3) * 4
    {
        return Err(Error::Invalid);
    }
    let body = general_purpose::STANDARD
        .decode(&request.body_base64)
        .map_err(|_| Error::Invalid)?;
    if body.len() > MAX_CALLBACK_BYTES {
        return Err(Error::Invalid);
    }
    let outcome = match (&request.verification, provider) {
        (
            Verification::Slack {
                signing_secret,
                team_id,
                app_id,
            },
            Provider::Slack,
        ) => slack(&request, &body, signing_secret, team_id, app_id, now)?,
        (
            Verification::Notion {
                verification_token,
                workspace_id,
                subscription_id,
                bot_id,
            },
            Provider::Notion,
        ) => notion(
            &request,
            &body,
            verification_token,
            workspace_id,
            subscription_id,
            bot_id,
        )?,
        (
            Verification::Drive {
                channel_token,
                channel_id,
                resource_id,
            },
            Provider::GoogleDrive,
        ) => drive(&request, &body, channel_token, channel_id, resource_id)?,
        (
            Verification::Graph {
                client_state,
                subscription_id,
                tenant_id,
            },
            Provider::Outlook | Provider::OneDrive | Provider::SharePoint | Provider::Teams,
        ) => graph(&request, &body, client_state, subscription_id, tenant_id)?,
        (
            Verification::Gmail {
                audience,
                email,
                mailbox,
                subscription,
                jwks,
            },
            Provider::Gmail,
        ) => gmail(
            &request,
            &body,
            audience,
            email,
            mailbox,
            subscription,
            jwks,
            now,
        )?,
        _ => return Err(Error::Invalid),
    };
    Ok(Response {
        context: request.context,
        outcome,
    })
}

fn valid_id(value: &str) -> bool {
    !value.is_empty() && value.len() <= 512 && !value.chars().any(char::is_control)
}
fn field<'a>(value: &'a Value, key: &str) -> Result<&'a str, Error> {
    value
        .get(key)
        .and_then(Value::as_str)
        .filter(|s| valid_id(s))
        .ok_or(Error::Invalid)
}
fn header<'a>(request: &'a Request, key: &str) -> Result<&'a str, Error> {
    request
        .headers
        .get(key)
        .map(String::as_str)
        .ok_or(Error::Unauthorized)
}
fn json(body: &[u8]) -> Result<Value, Error> {
    decode_control(body, MAX_CALLBACK_BYTES).map_err(|_| Error::Invalid)
}
fn notification(id: &str, kind: Change, container: &str, object: &str) -> Notification {
    Notification {
        event_id: id.to_owned(),
        kind,
        container_id: container.to_owned(),
        object_id: object.to_owned(),
    }
}
fn single(event: Notification) -> Outcome {
    Outcome::Notifications {
        events: vec![event],
    }
}

pub(crate) fn secret_equal(expected: &str, supplied: &str) -> bool {
    let mut left = Hmac::<Sha256>::new_from_slice(expected.as_bytes()).expect("HMAC key length");
    let mut right = Hmac::<Sha256>::new_from_slice(supplied.as_bytes()).expect("HMAC key length");
    left.update(b"nebula-secret-comparison-v1");
    right.update(b"nebula-secret-comparison-v1");
    left.finalize() == right.finalize()
}

fn signature(secret: &Secret, supplied: &str, prefix: &str, parts: &[&[u8]]) -> Result<(), Error> {
    if secret.0.len() < 16 || secret.0.len() > 4096 {
        return Err(Error::Unauthorized);
    }
    let hex = supplied
        .strip_prefix(prefix)
        .filter(|v| v.len() == 64)
        .ok_or(Error::Unauthorized)?;
    let mut digest = [0u8; 32];
    for (i, pair) in hex.as_bytes().as_chunks::<2>().0.iter().enumerate() {
        let digit = |b: u8| -> Result<u8, Error> {
            match b {
                b'0'..=b'9' => Ok(b - b'0'),
                b'a'..=b'f' => Ok(b - b'a' + 10),
                _ => Err(Error::Unauthorized),
            }
        };
        digest[i] = digit(pair[0])? * 16 + digit(pair[1])?;
    }
    let mut mac =
        Hmac::<Sha256>::new_from_slice(secret.0.as_bytes()).map_err(|_| Error::Unauthorized)?;
    for part in parts {
        mac.update(part);
    }
    mac.verify_slice(&digest).map_err(|_| Error::Unauthorized)
}

fn slack(
    request: &Request,
    body: &[u8],
    secret: &Secret,
    team: &str,
    app: &str,
    now: u64,
) -> Result<Outcome, Error> {
    let timestamp = header(request, "x-slack-request-timestamp")?;
    let time = timestamp.parse::<u64>().map_err(|_| Error::Unauthorized)?;
    if timestamp.len() > 20 || now.abs_diff(time) > 300 {
        return Err(Error::Unauthorized);
    }
    signature(
        secret,
        header(request, "x-slack-signature")?,
        "v0=",
        &[b"v0:", timestamp.as_bytes(), b":", body],
    )?;
    let value = json(body)?;
    if field(&value, "type")? == "url_verification" {
        if !request.verifying {
            return Err(Error::Unauthorized);
        }
        return Ok(Outcome::Challenge {
            value: field(&value, "challenge")?.to_owned(),
        });
    }
    if field(&value, "type")? != "event_callback" {
        return Ok(Outcome::Ignore);
    }
    let installation_matches = if let Some(authorizations) = value.get("authorizations") {
        let authorizations = authorizations
            .as_array()
            .filter(|v| !v.is_empty() && v.len() <= 1024)
            .ok_or(Error::Invalid)?;
        authorizations.iter().any(|authorization| {
            authorization.get("team_id").and_then(Value::as_str) == Some(team)
                && authorization
                    .get("is_enterprise_install")
                    .and_then(Value::as_bool)
                    != Some(true)
        })
    } else {
        field(&value, "team_id")? == team
    };
    if !installation_matches || field(&value, "api_app_id")? != app {
        return Err(Error::Unauthorized);
    }
    let id = field(&value, "event_id")?;
    let event = &value["event"];
    match field(event, "type")? {
        "app_uninstalled" => Ok(single(notification(id, Change::Revoked, "", ""))),
        "tokens_revoked" => Ok(single(notification(id, Change::Reauthorize, "", ""))),
        "message" => {
            let channel = field(event, "channel")?;
            let (kind, object) = match event.get("subtype").and_then(Value::as_str) {
                Some("message_deleted") => (Change::Deleted, field(event, "deleted_ts")?),
                Some("message_changed") => (Change::Changed, field(&event["message"], "ts")?),
                _ => (Change::Changed, field(event, "ts")?),
            };
            Ok(single(notification(id, kind, channel, object)))
        }
        "channel_deleted" | "channel_archive" => Ok(single(notification(
            id,
            Change::Reconcile,
            field(event, "channel")?,
            "",
        ))),
        _ => Ok(Outcome::Ignore),
    }
}

fn notion(
    request: &Request,
    body: &[u8],
    secret: &Secret,
    workspace: &str,
    subscription: &str,
    bot: &str,
) -> Result<Outcome, Error> {
    signature(
        secret,
        header(request, "x-notion-signature")?,
        "sha256=",
        &[body],
    )?;
    let value = json(body)?;
    if field(&value, "workspace_id")? != workspace
        || field(&value, "subscription_id")? != subscription
    {
        return Err(Error::Unauthorized);
    }
    if let Some(accessible) = value.get("accessible_by") {
        let entries = accessible
            .as_array()
            .filter(|v| v.len() <= 1024)
            .ok_or(Error::Invalid)?;
        if !entries
            .iter()
            .any(|v| v.get("id").and_then(Value::as_str) == Some(bot))
        {
            return Ok(Outcome::Ignore);
        }
    }
    let kind = match field(&value, "type")? {
        "page.deleted" | "database.deleted" | "data_source.deleted" => Change::Deleted,
        "page.created"
        | "page.content_updated"
        | "page.properties_updated"
        | "page.moved"
        | "page.undeleted"
        | "database.created"
        | "database.moved"
        | "database.content_updated"
        | "database.schema_updated"
        | "database.undeleted"
        | "data_source.created"
        | "data_source.moved"
        | "data_source.content_updated"
        | "data_source.schema_updated"
        | "data_source.undeleted" => Change::Changed,
        _ => return Ok(Outcome::Ignore),
    };
    Ok(single(notification(
        field(&value, "id")?,
        kind,
        "",
        field(&value["entity"], "id")?,
    )))
}

fn drive(
    request: &Request,
    body: &[u8],
    secret: &Secret,
    channel: &str,
    resource: &str,
) -> Result<Outcome, Error> {
    if !body.is_empty()
        || secret.0.len() < 16
        || secret.0.len() > 4096
        || !secret_equal(&secret.0, header(request, "x-goog-channel-token")?)
        || header(request, "x-goog-channel-id")? != channel
        || header(request, "x-goog-resource-id")? != resource
    {
        return Err(Error::Unauthorized);
    }
    let number = header(request, "x-goog-message-number")?;
    if number.len() > 32 || number.is_empty() || !number.bytes().all(|c| c.is_ascii_digit()) {
        return Err(Error::Invalid);
    }
    match header(request, "x-goog-resource-state")? {
        "sync" | "add" | "remove" | "update" | "trash" | "untrash" | "change" | "changed" => {
            let id = stable_id(
                "drive-notification",
                &[channel.as_bytes(), number.as_bytes()],
            );
            Ok(single(notification(&id, Change::Reconcile, "", "")))
        }
        _ => Ok(Outcome::Ignore),
    }
}

fn graph(
    request: &Request,
    body: &[u8],
    secret: &Secret,
    subscription: &str,
    tenant: &str,
) -> Result<Outcome, Error> {
    if let Some(token) = request.query.get("validationToken") {
        if !request.verifying
            || token.is_empty()
            || token.len() > 4096
            || token.contains(['\r', '\n', '\0'])
        {
            return Err(Error::Unauthorized);
        }
        return Ok(Outcome::Challenge {
            value: token.clone(),
        });
    }
    if secret.0.len() < 16 || secret.0.len() > 128 {
        return Err(Error::Unauthorized);
    }
    let value = json(body)?;
    if value.get("validationTokens").is_some() {
        return Err(Error::Unsupported);
    }
    let events = value
        .get("value")
        .and_then(Value::as_array)
        .filter(|v| !v.is_empty() && v.len() <= 1000)
        .ok_or(Error::Invalid)?;
    let mut notices = Vec::with_capacity(events.len());
    for event in events {
        if field(event, "subscriptionId")? != subscription
            || field(event, "tenantId")? != tenant
            || !secret_equal(&secret.0, field(event, "clientState")?)
        {
            return Err(Error::Unauthorized);
        }
        if event.get("encryptedContent").is_some() {
            return Err(Error::Unsupported);
        }
        let kind = if let Some(lifecycle) = event.get("lifecycleEvent").and_then(Value::as_str) {
            match lifecycle {
                "reauthorizationRequired" => Change::Reauthorize,
                "subscriptionRemoved" | "missed" => Change::Missed,
                _ => return Err(Error::Unsupported),
            }
        } else {
            match field(event, "changeType")? {
                "created" | "updated" => Change::Changed,
                "deleted" => Change::Deleted,
                _ => return Err(Error::Unsupported),
            }
        };
        let object = event
            .get("resourceData")
            .and_then(|v| v.get("id"))
            .and_then(Value::as_str)
            .unwrap_or("");
        if object.len() > 512 || object.chars().any(char::is_control) {
            return Err(Error::Invalid);
        }
        // Graph can send identical bodies for distinct edits. Use this callback's
        // persisted attempt, not a payload hash that could suppress a later edit.
        let id = stable_id(
            "graph-notification",
            &[
                request.context.attempt_id.as_bytes(),
                &(notices.len() as u64).to_be_bytes(),
            ],
        );
        let resource = event.get("resource").and_then(Value::as_str).unwrap_or("");
        if resource.len() > 512 || resource.chars().any(char::is_control) {
            return Err(Error::Invalid);
        }
        notices.push(notification(&id, kind, resource, object));
    }
    Ok(Outcome::Notifications { events: notices })
}

#[allow(
    clippy::too_many_arguments,
    reason = "Provider verification material is operation-scoped and has no global credentials"
)]
fn gmail(
    request: &Request,
    body: &[u8],
    audience: &str,
    email: &str,
    mailbox: &str,
    subscription: &str,
    jwks: &JwkSet,
    now: u64,
) -> Result<Outcome, Error> {
    if audience.is_empty()
        || audience.len() > 4096
        || !valid_id(email)
        || !valid_id(mailbox)
        || !valid_id(subscription)
        || jwks.keys.is_empty()
        || jwks.keys.len() > 16
    {
        return Err(Error::Invalid);
    }
    let token = header(request, "authorization")?
        .strip_prefix("Bearer ")
        .filter(|t| t.len() <= 16384)
        .ok_or(Error::Unauthorized)?;
    let header = decode_header(token).map_err(|_| Error::Unauthorized)?;
    if header.alg != Algorithm::RS256 {
        return Err(Error::Unauthorized);
    }
    let kid = header
        .kid
        .as_deref()
        .filter(|s| valid_id(s))
        .ok_or(Error::Unauthorized)?;
    let key = jwks.find(kid).ok_or(Error::Unauthorized)?;
    let jsonwebtoken::jwk::AlgorithmParameters::RSA(rsa) = &key.algorithm else {
        return Err(Error::Unauthorized);
    };
    if !(342..=684).contains(&rsa.n.len()) || rsa.e.len() > 8 {
        return Err(Error::Unauthorized);
    }
    let key = DecodingKey::from_jwk(key).map_err(|_| Error::Unauthorized)?;
    let mut validation = Validation::new(Algorithm::RS256);
    validation.set_audience(&[audience]);
    validation.set_issuer(&["https://accounts.google.com", "accounts.google.com"]);
    validation.set_required_spec_claims(&["exp", "iat", "iss", "aud"]);
    validation.validate_exp = false;
    validation.validate_nbf = false;
    #[derive(Deserialize, Clone)]
    struct Claims {
        email: String,
        email_verified: bool,
        iat: u64,
        exp: u64,
        nbf: Option<u64>,
    }
    let claims = decode::<Claims>(token, &key, &validation)
        .map_err(|_| Error::Unauthorized)?
        .claims;
    if claims.email != email
        || !claims.email_verified
        || claims.iat > now.saturating_add(30)
        || claims.exp <= claims.iat
        || claims.exp.saturating_add(30) <= now
        || claims
            .nbf
            .is_some_and(|start| start > now.saturating_add(30))
    {
        return Err(Error::Unauthorized);
    }
    let value = json(body)?;
    if field(&value, "subscription")? != subscription {
        return Err(Error::Unauthorized);
    }
    let message = &value["message"];
    let encoded = message
        .get("data")
        .and_then(Value::as_str)
        .filter(|s| s.len() <= 4096)
        .ok_or(Error::Invalid)?;
    let decoded = general_purpose::URL_SAFE
        .decode(encoded)
        .or_else(|_| general_purpose::URL_SAFE_NO_PAD.decode(encoded))
        .map_err(|_| Error::Invalid)?;
    let data = json(&decoded)?;
    if field(&data, "emailAddress")? != mailbox {
        return Err(Error::Unauthorized);
    }
    let history = field(&data, "historyId")?;
    if !history.bytes().all(|c| c.is_ascii_digit()) {
        return Err(Error::Invalid);
    }
    Ok(single(notification(
        field(message, "messageId")?,
        Change::Reconcile,
        "",
        "",
    )))
}
