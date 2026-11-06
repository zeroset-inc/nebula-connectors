//! Context checks shared by the Google adapters.

use nebula_connector_sdk::{ConnectorError, Context, client::CredentialMode};
use serde_json::json;

use crate::common::Result;

/// Google IDs are letters, digits, hyphens and underscores; the broker relies
/// on that to place them inside quoted Google queries.
pub(crate) fn google_id(value: &str) -> bool {
    (1..=256).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
}

/// Google adapters run managed, complete inventories of selected containers
/// for a Google account, whose authority is its numeric ID.
pub(crate) fn validate_context(context: &Context) -> Result<()> {
    let authority = context.authority.as_bytes();
    if context.credential_mode != CredentialMode::Managed
        || !context.complete_inventory
        || context.config != json!({})
        || !(1..=64).contains(&authority.len())
        || !authority.iter().all(u8::is_ascii_digit)
        || context.scope.containers().iter().any(|id| !google_id(id))
    {
        return Err(ConnectorError::Configuration);
    }
    Ok(())
}
