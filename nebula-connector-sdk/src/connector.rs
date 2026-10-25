use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::{Client, Run, protocol::ConnectorEvent};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Specification {
    pub provider: String,
    pub version: String,
    pub cursor_version: u32,
    pub config_schema: Value,
    pub capabilities: Capabilities,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Capabilities {
    pub managed: bool,
    pub incremental: bool,
    pub complete_inventory: bool,
    pub permissions: bool,
    pub original_files: bool,
    pub supported_filters: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Resource {
    pub id: String,
    pub name: String,
    pub kind: String,
    pub parent_id: Option<String>,
}

pub struct ResourcePage {
    pub resources: Vec<Resource>,
    pub next: Option<Value>,
}

/// At most 98 events; the runner adds the accepted checkpoint and optional scan seal.
/// A failed item needs an explicit observation; it cannot silently disappear.
pub struct Page {
    pub events: Vec<ConnectorEvent>,
    pub cursor: Value,
    pub complete: bool,
}

pub struct Context {
    pub service: Client,
    pub run: Run,
    pub config: Value,
    pub authority: String,
    pub scope: crate::scope::Filter,
    pub complete_inventory: bool,
    pub credential_mode: crate::client::CredentialMode,
}

#[derive(Debug, thiserror::Error)]
pub enum ConnectorError {
    #[error("provider_configuration_invalid")]
    Configuration,
    #[error("provider_permission_denied")]
    Permission,
    #[error("provider_response_invalid")]
    Response,
    #[error("provider_cursor_expired")]
    CursorExpired,
    #[error("provider_inventory_incomplete")]
    IncompleteInventory,
    #[error("connector_transport_failed")]
    Transport(#[from] crate::ClientError),
}

/// Adapter code maps provider data. The service owns credentials, scheduling,
/// fencing, durable handoff, absence reconciliation and ingestion settlement.
#[async_trait]
pub trait Connector: Send + Sync {
    fn specification(&self) -> Specification;
    async fn check(&self, context: &Context) -> Result<(), ConnectorError>;
    async fn discover(
        &self,
        context: &Context,
        cursor: Option<&Value>,
    ) -> Result<ResourcePage, ConnectorError>;
    async fn read(&self, context: &Context, cursor: &Value) -> Result<Page, ConnectorError>;
}

/// Length-prefix every component so provider identifiers cannot alias one another.
/// Include the provider, authority, object, operation and revision in change IDs.
pub fn stable_id(domain: &str, parts: &[&[u8]]) -> String {
    let mut hash = Sha256::new();
    for part in std::iter::once(domain.as_bytes()).chain(parts.iter().copied()) {
        hash.update((part.len() as u64).to_be_bytes());
        hash.update(part);
    }
    hash.finalize().iter().map(|b| format!("{b:02x}")).collect()
}
