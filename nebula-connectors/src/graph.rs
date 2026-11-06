//! Context checks and paging shared by the Microsoft Graph adapters.

use nebula_connector_sdk::{ConnectorError, Context, client::CredentialMode};
use serde_json::{Value, json};

use crate::common::{Result, percent_decode, request};

/// Microsoft work accounts are identified by their Entra object ID, a GUID.
fn object_id(value: &str) -> bool {
    value.len() == 36
        && value.bytes().enumerate().all(|(index, byte)| {
            if [8, 13, 18, 23].contains(&index) {
                byte == b'-'
            } else {
                byte.is_ascii_hexdigit()
            }
        })
}

/// Graph item, folder, drive and channel IDs, with "~" joining a Teams team
/// and channel into one container.
pub(crate) fn graph_id(value: &str) -> bool {
    (1..=1024).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"!=_.,:@~-".contains(&byte))
}

/// Microsoft adapters run managed, complete inventories of selected
/// containers for a work account.
pub(crate) fn validate_context(context: &Context) -> Result<()> {
    if context.credential_mode != CredentialMode::Managed
        || !context.complete_inventory
        || context.config != json!({})
        || !object_id(&context.authority)
        || context.scope.containers().iter().any(|id| !graph_id(id))
    {
        return Err(ConnectorError::Configuration);
    }
    Ok(())
}

/// Confirms the token still belongs to the connection's account.
pub(crate) async fn check_account(context: &Context) -> Result<()> {
    validate_context(context)?;
    let me = request(context, "me.get", json!({"select":"id"})).await?;
    if me["id"].as_str() != Some(context.authority.as_str()) {
        return Err(ConnectorError::Permission);
    }
    Ok(())
}

/// The page token in a Graph next link, as "skiptoken=…" or "skip=…". The
/// broker cannot follow the link itself, so the token returns as an argument.
pub(crate) fn next_page(response: &Value) -> Result<Option<Value>> {
    let link = match response.get("@odata.nextLink") {
        None | Some(Value::Null) => return Ok(None),
        Some(Value::String(link)) => link,
        _ => return Err(ConnectorError::Response),
    };
    let (_, query) = link
        .split_once('?')
        .ok_or(ConnectorError::IncompleteInventory)?;
    for pair in query.split('&') {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        let key = percent_decode(key)?;
        if let Some(name @ ("skiptoken" | "skip")) = key.strip_prefix('$') {
            let value = percent_decode(value)?;
            if value.is_empty() || value.len() > 4000 {
                return Err(ConnectorError::IncompleteInventory);
            }
            return Ok(Some(format!("{name}={value}").into()));
        }
    }
    Err(ConnectorError::IncompleteInventory)
}

/// Adds the page a cursor names to a listing's arguments.
pub(crate) fn page_arguments(page: &str, arguments: &mut Value) -> Result<()> {
    if page.is_empty() {
        return Ok(());
    }
    match page.split_once('=') {
        Some((name @ ("skiptoken" | "skip"), value)) if !value.is_empty() => {
            arguments[name] = value.into();
            Ok(())
        }
        _ => Err(ConnectorError::CursorExpired),
    }
}

/// Graph timestamps, such as 2026-10-27T18:46:26Z, in milliseconds.
pub(crate) fn graph_time(value: &Value) -> Result<i64> {
    crate::common::timestamp_ms(value.as_str().ok_or(ConnectorError::Response)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn next_links_become_page_arguments() {
        let page = next_page(&json!({"@odata.nextLink":"https://graph.microsoft.com/v1.0/me/messages?%24select=id&%24skip=10"}))
            .unwrap()
            .unwrap();
        assert_eq!(page, "skip=10");
        let mut arguments = json!({"folder":"inbox"});
        page_arguments(page.as_str().unwrap(), &mut arguments).unwrap();
        assert_eq!(arguments, json!({"folder":"inbox","skip":"10"}));
        let token = next_page(&json!({"@odata.nextLink":"https://graph.microsoft.com/v1.0/drives/b!1/root/children?$skiptoken=UGFnZWQ9VFJVRQ%3d%3d"}))
            .unwrap()
            .unwrap();
        assert_eq!(token, "skiptoken=UGFnZWQ9VFJVRQ==");
        assert!(next_page(&json!({})).unwrap().is_none());
        assert!(
            next_page(
                &json!({"@odata.nextLink":"https://graph.microsoft.com/v1.0/me/messages?$top=4"})
            )
            .is_err()
        );
        assert!(page_arguments("cursor=1", &mut arguments).is_err());
    }

    #[test]
    fn accounts_are_entra_object_ids() {
        assert!(object_id("48d31887-5fad-4d73-a9f5-3c356e68a038"));
        assert!(!object_id("48d318875fad4d73a9f53c356e68a038"));
        assert!(!object_id("1077"));
    }
}
