//! Context checks, paging and document text shared by the Atlassian adapters.

use nebula_connector_sdk::{ConnectorError, Context, client::CredentialMode};
use serde_json::{Value, json};

use crate::common::{Result, percent_decode, request};

/// Atlassian account IDs, such as 557058:f1b2c3 or 5b10ac8d82e05b22cc7d4ef5.
fn account_id(value: &str) -> bool {
    (1..=128).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b":-_".contains(&byte))
}

/// Splits a "site~item" container into the site's cloud ID and the space or
/// project inside it.
pub(crate) fn split(container: &str) -> Result<(&str, &str)> {
    let (cloud, item) = container
        .split_once('~')
        .ok_or(ConnectorError::Configuration)?;
    let cloud_ok = cloud.len() == 36
        && cloud
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase() || byte == b'-');
    let item_ok = (1..=64).contains(&item.len())
        && item
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_');
    if cloud_ok && item_ok {
        Ok((cloud, item))
    } else {
        Err(ConnectorError::Configuration)
    }
}

/// Atlassian adapters run managed, complete inventories of selected
/// containers for an Atlassian account.
pub(crate) fn validate_context(context: &Context) -> Result<()> {
    if context.credential_mode != CredentialMode::Managed
        || !context.complete_inventory
        || context.config != json!({})
        || !account_id(&context.authority)
        || context
            .scope
            .containers()
            .iter()
            .any(|container| split(container).is_err())
    {
        return Err(ConnectorError::Configuration);
    }
    Ok(())
}

/// Confirms the token still belongs to the connection's account.
pub(crate) async fn check_account(context: &Context) -> Result<()> {
    validate_context(context)?;
    let me = request(context, "me.get", json!({})).await?;
    if me["account_id"].as_str() != Some(context.authority.as_str()) {
        return Err(ConnectorError::Permission);
    }
    Ok(())
}

/// The cursor in a Confluence next link, which the broker cannot follow.
pub(crate) fn next_cursor(response: &Value) -> Result<Option<Value>> {
    let link = match &response["_links"]["next"] {
        Value::Null => return Ok(None),
        Value::String(link) => link,
        _ => return Err(ConnectorError::Response),
    };
    let (_, query) = link
        .split_once('?')
        .ok_or(ConnectorError::IncompleteInventory)?;
    for pair in query.split('&') {
        if let Some(("cursor", value)) = pair.split_once('=') {
            let value = percent_decode(value)?;
            if value.is_empty() || value.len() > 2048 {
                return Err(ConnectorError::IncompleteInventory);
            }
            return Ok(Some(value.into()));
        }
    }
    Err(ConnectorError::IncompleteInventory)
}

/// Plain text from an Atlassian Document Format node, one line per block.
pub(crate) fn document_text(node: &Value) -> String {
    let mut text = String::new();
    write_node(node, &mut text, 0);
    text.trim_end().to_owned()
}

fn write_node(node: &Value, text: &mut String, depth: usize) {
    if depth > 32 {
        return;
    }
    match node["type"].as_str() {
        Some("text") => text.push_str(node["text"].as_str().unwrap_or_default()),
        Some("hardBreak") => text.push('\n'),
        Some("mention" | "emoji") => {
            text.push_str(node["attrs"]["text"].as_str().unwrap_or_default())
        }
        Some("inlineCard") => text.push_str(node["attrs"]["url"].as_str().unwrap_or_default()),
        kind => {
            if kind == Some("listItem") {
                text.push_str("- ");
            }
            for child in node["content"].as_array().into_iter().flatten() {
                write_node(child, text, depth + 1);
            }
            if matches!(
                kind,
                Some(
                    "paragraph"
                        | "heading"
                        | "listItem"
                        | "codeBlock"
                        | "blockquote"
                        | "rule"
                        | "tableRow"
                )
            ) && !text.ends_with('\n')
            {
                text.push('\n');
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn documents_become_lines_and_next_links_become_cursors() {
        let document = json!({"type":"doc","content":[
            {"type":"paragraph","content":[{"type":"text","text":"Ship "},{"type":"mention","attrs":{"text":"@Ada"}}]},
            {"type":"bulletList","content":[{"type":"listItem","content":[{"type":"paragraph","content":[{"type":"text","text":"docs"}]}]}]}]});
        assert_eq!(document_text(&document), "Ship @Ada\n- docs");
        assert_eq!(
            next_cursor(
                &json!({"_links":{"next":"/wiki/api/v2/spaces/100/pages?cursor=abc%3D%3D&limit=4"}})
            )
            .unwrap()
            .unwrap(),
            "abc=="
        );
        assert!(next_cursor(&json!({"_links":{}})).unwrap().is_none());
        assert!(split("11111111-2222-3333-4444-555555555555~OPS").is_ok());
        assert!(split("site~OPS").is_err());
    }
}
