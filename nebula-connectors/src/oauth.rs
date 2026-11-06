//! Reviewed provider OAuth planning and bounded response interpretation.
//! The broker injects credentials and owns every network request and journal.
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use zeroize::Zeroize;

pub const MAX_RESPONSE_BYTES: usize = 65_536;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Context {
    pub version: u8,
    pub attempt_id: String,
    pub tenant_id: String,
    pub connection_id: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub context: Context,
    pub action: Action,
    #[serde(default)]
    pub response: Option<ProviderResponse>,
    #[serde(default)]
    pub required_scopes: Vec<String>,
    #[serde(default)]
    pub verify_scopes: bool,
}

#[derive(Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Action {
    Exchange,
    Refresh,
    Identity,
    Revoke,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderResponse {
    pub status: u16,
    pub body: String,
}

impl Drop for ProviderResponse {
    fn drop(&mut self) {
        self.body.zeroize();
    }
}

#[derive(Serialize)]
pub struct Response {
    pub context: Context,
    pub profile: Profile,
    pub profile_digest: String,
    pub result: Option<Parsed>,
}

#[derive(Serialize)]
pub struct Profile {
    /// S256 support for this approved client flow. Slack remains a confidential
    /// bot client: its PKCE opt-in changes app type and refresh-token lifetime.
    pub pkce: bool,
    pub token_url: &'static str,
    pub identity_url: &'static str,
    pub identity_method: &'static str,
    pub revoke_url: &'static str,
    pub revoke_token: &'static str,
    pub revoke_scope: &'static str,
    pub client_auth: &'static str,
    pub token_body: &'static str,
    pub protocol: &'static str,
    pub identity_id: &'static str,
    pub identity_label: &'static str,
}

#[derive(Serialize, Default)]
pub struct Parsed {
    pub kind: &'static str,
    pub access_token: String,
    pub refresh_token: String,
    pub expires_in: u64,
    pub account_id: String,
    pub account_label: String,
}

impl Drop for Parsed {
    fn drop(&mut self) {
        self.access_token.zeroize();
        self.refresh_token.zeroize();
    }
}

fn profile(provider: &str) -> Option<Profile> {
    let (token_url, identity_url, identity_id, identity_label) = match provider {
        "slack" => ("https://slack.com/api/oauth.v2.access", "", "id", "name"),
        "google_drive" | "gmail" => (
            "https://oauth2.googleapis.com/token",
            "https://openidconnect.googleapis.com/v1/userinfo",
            "sub",
            "email",
        ),
        "onedrive" | "sharepoint" | "outlook" | "teams" => (
            "https://login.microsoftonline.com/organizations/oauth2/v2.0/token",
            "https://graph.microsoft.com/v1.0/me",
            "id",
            "userPrincipalName",
        ),
        "notion" => (
            "https://api.notion.com/v1/oauth/token",
            "",
            "workspace_id",
            "workspace_name",
        ),
        "confluence" | "jira" => (
            "https://auth.atlassian.com/oauth/token",
            "https://api.atlassian.com/me",
            "account_id",
            "email",
        ),
        "box" => (
            "https://api.box.com/oauth2/token",
            "https://api.box.com/2.0/users/me",
            "id",
            "login",
        ),
        "dropbox" => (
            "https://api.dropboxapi.com/oauth2/token",
            "https://api.dropboxapi.com/2/users/get_current_account",
            "account_id",
            "email",
        ),
        "github" => (
            "https://github.com/login/oauth/access_token",
            "https://api.github.com/user",
            "id",
            "login",
        ),
        _ => return None,
    };
    Some(Profile {
        pkce: matches!(
            provider,
            "google_drive"
                | "gmail"
                | "onedrive"
                | "sharepoint"
                | "outlook"
                | "teams"
                | "dropbox"
                | "github"
        ),
        token_url,
        identity_url,
        identity_id,
        identity_label,
        identity_method: if provider == "dropbox" { "POST" } else { "GET" },
        revoke_url: match provider {
            "slack" => "https://slack.com/api/auth.revoke",
            "google_drive" | "gmail" => "https://oauth2.googleapis.com/revoke",
            _ => "",
        },
        revoke_scope: match provider {
            "slack" => "token",
            "google_drive" | "gmail" => "grant",
            _ => "unsupported",
        },
        revoke_token: match provider {
            "slack" => "both",
            "google_drive" | "gmail" => "refresh_or_access",
            _ => "none",
        },
        client_auth: if matches!(provider, "slack" | "notion") {
            "basic"
        } else {
            "post"
        },
        token_body: if matches!(provider, "notion" | "confluence" | "jira") {
            "json"
        } else {
            "form"
        },
        protocol: if provider == "slack" {
            "slack_oauth_v2"
        } else {
            "oauth2"
        },
    })
}

fn bounded(value: &str, max: usize) -> bool {
    !value.is_empty() && value.len() <= max && !value.chars().any(char::is_control)
}
fn text(value: &Value, field: &str) -> String {
    value
        .get(field)
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_owned()
}
fn identity_value(value: &Value, field: &str) -> String {
    match value.get(field) {
        Some(Value::String(value)) => value.clone(),
        Some(Value::Number(value)) => value.as_u64().map(|n| n.to_string()).unwrap_or_default(),
        _ => String::new(),
    }
}
fn account(profile: &Profile, value: &Value, output: &mut Parsed) -> bool {
    output.account_id = identity_value(value, profile.identity_id);
    output.account_label = identity_value(value, profile.identity_label);
    if output.account_label.is_empty() {
        output.account_label.clone_from(&output.account_id);
    }
    bounded(&output.account_id, 512) && bounded(&output.account_label, 256)
}

pub fn process(provider: &str, request: Request) -> Result<Response, &'static str> {
    if request.context.version != 1
        || !bounded(&request.context.attempt_id, 128)
        || !bounded(&request.context.tenant_id, 512)
        || !bounded(&request.context.connection_id, 512)
        || request.required_scopes.len() > 64
        || request.required_scopes.iter().any(|s| !bounded(s, 256))
    {
        return Err("invalid_oauth_request");
    }
    let profile = profile(provider).ok_or("oauth_provider_not_supported")?;
    let encoded = serde_json::to_vec(&profile).map_err(|_| "invalid_oauth_profile")?;
    let profile_digest = format!("{:x}", Sha256::digest(encoded));
    let result = request
        .response
        .as_ref()
        .map(|response| parse(provider, &profile, &request, response));
    Ok(Response {
        context: request.context,
        profile,
        profile_digest,
        result,
    })
}

fn parse(
    provider: &str,
    profile: &Profile,
    request: &Request,
    response: &ProviderResponse,
) -> Parsed {
    let mut output = Parsed::default();
    output.kind = "uncertain";
    if response.body.len() > MAX_RESPONSE_BYTES || !(100..=599).contains(&response.status) {
        return output;
    }
    if request.action == Action::Revoke && (200..=299).contains(&response.status) {
        if provider != "slack"
            || serde_json::from_str::<Value>(&response.body)
                .ok()
                .and_then(|v| v.get("ok").and_then(Value::as_bool))
                == Some(true)
            || serde_json::from_str::<Value>(&response.body)
                .ok()
                .and_then(|v| v.get("error").and_then(Value::as_str).map(str::to_owned))
                .is_some_and(|e| matches!(e.as_str(), "token_revoked"))
        {
            output.kind = "revoked";
        }
        return output;
    }
    let Ok(value) = serde_json::from_str::<Value>(&response.body) else {
        return output;
    };
    if request.action == Action::Identity {
        if response.status == 200 && account(profile, &value, &mut output) {
            output.kind = "identity";
        }
        return output;
    }
    output.access_token = text(&value, "access_token");
    output.refresh_token = text(&value, "refresh_token");
    let error = text(&value, "error");
    if output.access_token.is_empty() && output.refresh_token.is_empty() {
        output.kind = match (response.status, error.as_str()) {
            (400, "invalid_grant") => "refused",
            (
                400 | 401,
                "invalid_request"
                | "invalid_client"
                | "unauthorized_client"
                | "unsupported_grant_type"
                | "invalid_scope",
            ) => "retryable",
            _ => "uncertain",
        };
        return output;
    }
    // Keep valid issued tokens even when the rest of the response is refused:
    // the broker must retain them for provider revocation rather than lose custody.
    if !bounded(&output.access_token, 16_384) {
        output.access_token.clear();
        return output;
    }
    if !output.refresh_token.is_empty() && !bounded(&output.refresh_token, 16_384) {
        output.refresh_token.clear();
        return output;
    }
    let token_type = text(&value, "token_type");
    let valid_type = if provider == "slack" {
        value.get("ok").and_then(Value::as_bool) == Some(true) && token_type == "bot"
    } else {
        token_type.eq_ignore_ascii_case("bearer")
    };
    let expires = match value.get("expires_in") {
        None => 0,
        Some(value) => match value.as_u64() {
            Some(n) => n,
            None => return output,
        },
    };
    if !error.is_empty()
        || response.status != 200
        || !valid_type
        || request.action == Action::Refresh
            && (expires == 0 || provider == "slack" && output.refresh_token.is_empty())
        || request.action == Action::Exchange && expires > 0 && output.refresh_token.is_empty()
    {
        return output;
    }
    if request.verify_scopes {
        let scopes = text(&value, "scope");
        if request.required_scopes.iter().any(|s| {
            !scopes
                .split(|c: char| c.is_ascii_whitespace() || c == ',')
                .any(|granted| granted == s)
        }) {
            return output;
        }
    }
    output.expires_in = expires.min(366 * 86_400);
    if request.action == Action::Exchange && profile.identity_url.is_empty() {
        let account_value = if provider == "slack" {
            if value.get("is_enterprise_install").and_then(Value::as_bool) == Some(true) {
                return output;
            }
            let Some(team) = value.get("team") else {
                return output;
            };
            team
        } else {
            &value
        };
        if !account(profile, account_value, &mut output) {
            return output;
        }
    }
    output.kind = "tokens";
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    fn request(action: Action, status: u16, body: &str) -> Request {
        Request {
            context: Context {
                version: 1,
                attempt_id: "attempt".into(),
                tenant_id: "tenant".into(),
                connection_id: "connection".into(),
            },
            action,
            response: Some(ProviderResponse {
                status,
                body: body.into(),
            }),
            required_scopes: vec![],
            verify_scopes: false,
        }
    }
    #[test]
    fn numeric_identity_and_ambiguous_token_responses_preserve_exact_evidence() {
        let result = process(
            "github",
            request(
                Action::Identity,
                200,
                r#"{"id":9007199254740993,"login":"test"}"#,
            ),
        )
        .unwrap()
        .result
        .unwrap();
        assert_eq!(result.account_id, "9007199254740993");
        let result = process(
            "slack",
            request(
                Action::Refresh,
                500,
                r#"{"access_token":"issued","refresh_token":"rotated"}"#,
            ),
        )
        .unwrap()
        .result
        .unwrap();
        assert_eq!(result.kind, "uncertain");
        assert_eq!(result.refresh_token, "rotated");
        assert_eq!(
            process(
                "gmail",
                request(Action::Refresh, 400, r#"{"error":"invalid_grant"}"#)
            )
            .unwrap()
            .result
            .unwrap()
            .kind,
            "refused"
        );
        assert_eq!(
            process(
                "gmail",
                request(
                    Action::Refresh,
                    400,
                    r#"{"error":"invalid_grant","access_token":"issued"}"#
                )
            )
            .unwrap()
            .result
            .unwrap()
            .kind,
            "uncertain"
        );
    }
    #[test]
    fn pkce_is_an_approved_provider_capability() {
        for provider in [
            "google_drive",
            "gmail",
            "onedrive",
            "sharepoint",
            "outlook",
            "teams",
            "dropbox",
            "github",
        ] {
            assert!(profile(provider).unwrap().pkce, "{provider}");
        }
        for provider in ["slack", "notion", "confluence", "jira", "box"] {
            assert!(!profile(provider).unwrap().pkce, "{provider}");
        }
    }
    #[test]
    fn cleanup_distinguishes_token_revocation_from_account_wide_revocation() {
        let slack = profile("slack").unwrap();
        assert_eq!((slack.revoke_scope, slack.revoke_token), ("token", "both"));
        assert_eq!(profile("google_drive").unwrap().revoke_scope, "grant");
        assert_eq!(profile("notion").unwrap().revoke_scope, "unsupported");
        for (body, expected) in [
            (r#"{"ok":true,"revoked":true}"#, "revoked"),
            (r#"{"ok":false,"error":"token_revoked"}"#, "revoked"),
            (r#"{"ok":false,"error":"invalid_auth"}"#, "uncertain"),
        ] {
            assert_eq!(
                process("slack", request(Action::Revoke, 200, body))
                    .unwrap()
                    .result
                    .unwrap()
                    .kind,
                expected
            );
        }
    }
}
