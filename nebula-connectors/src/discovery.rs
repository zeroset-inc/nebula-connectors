//! Setup discovery maps approved catalog operations and their replies. The broker
//! owns HTTP, credentials, quotas and durable request accounting.
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};
use serde_json::Value;

mod atlassian;
mod common;
mod files;
mod google;
mod microsoft;
mod notion;
mod slack;

// Largest response admitted by the reviewed setup catalogs.
pub const MAX_BODY_BYTES: usize = 2 << 20;
pub const MAX_RESULT_BYTES: usize = 2 << 20;
pub const MAX_RPC_BYTES: usize = MAX_BODY_BYTES + (32 << 10);
pub const MAX_PAGES: usize = 5;
pub const SEARCH_MATCHES: usize = 200;

type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Deserialize, Serialize, PartialEq, Eq)]
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
    pub parent: String,
    pub query: String,
    pub cursor: String,
    pub pages: usize,
    pub matched: usize,
    pub response: Option<Box<serde_json::value::RawValue>>,
}

#[derive(Serialize)]
pub struct Response {
    pub context: Context,
    pub operation: Option<Operation>,
    pub page: Option<Page>,
    pub error: Option<Error>,
}

#[derive(Serialize)]
pub struct Operation {
    pub name: &'static str,
    pub arguments: Value,
}

#[derive(Serialize)]
pub struct Page {
    pub items: Vec<Resource>,
    pub cursor: String,
    pub continue_scan: bool,
}

#[derive(Serialize)]
pub struct Resource {
    pub id: String,
    pub name: String,
    pub kind: &'static str,
    pub parent_id: Option<String>,
    pub has_children: bool,
    pub selectable: bool,
}

#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Error {
    InvalidCursor,
    Revoked,
    RateLimited,
    Unavailable,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    version: u8,
    provider: String,
    parent: String,
    query: String,
    next: String,
}

fn valid_id(value: &str) -> bool {
    !value.is_empty() && value.len() <= 512 && !value.chars().any(char::is_control)
}

pub fn process(provider: &str, input: Request) -> std::result::Result<Response, &'static str> {
    if input.context.version != 1
        || !valid_id(&input.context.attempt_id)
        || !valid_id(&input.context.tenant_id)
        || !valid_id(&input.context.connection_id)
        || input.parent.len() > 256
        || input.query.len() > 256
        || input.cursor.len() > 4096
        || input.pages >= MAX_PAGES
        || input.matched > SEARCH_MATCHES
        || input
            .response
            .as_ref()
            .is_some_and(|body| body.get().len() > MAX_BODY_BYTES)
    {
        return Err("invalid_discovery_request");
    }
    let result = execute(provider, &input);
    let mut response = Response {
        context: input.context,
        operation: None,
        page: None,
        error: None,
    };
    match result {
        Ok((operation, page)) => {
            response.operation = operation;
            response.page = page;
        }
        Err(error) => response.error = Some(error),
    }
    Ok(response)
}

fn execute(provider: &str, input: &Request) -> Result<(Option<Operation>, Option<Page>)> {
    let next = if input.cursor.is_empty() {
        String::new()
    } else {
        let bytes = URL_SAFE_NO_PAD
            .decode(&input.cursor)
            .map_err(|_| Error::InvalidCursor)?;
        let cursor: Cursor = serde_json::from_slice(&bytes).map_err(|_| Error::InvalidCursor)?;
        if cursor.version != 1
            || cursor.provider != provider
            || cursor.parent != input.parent
            || cursor.query != input.query
            || cursor.next.is_empty()
        {
            return Err(Error::InvalidCursor);
        }
        cursor.next
    };
    let Some(operation) = plan(provider, &input.parent, &input.query, &next)? else {
        return Ok((
            None,
            Some(Page {
                items: vec![],
                cursor: String::new(),
                continue_scan: false,
            }),
        ));
    };
    let Some(body) = &input.response else {
        return Ok((Some(operation), None));
    };
    let value: Value = serde_json::from_str(body.get()).map_err(|_| Error::Unavailable)?;
    let (items, following) = interpret(provider, &input.parent, &input.query, &next, &value)?;
    let cursor = if following.is_empty() {
        String::new()
    } else {
        if following == next {
            return Err(Error::Unavailable);
        }
        let bytes = serde_json::to_vec(&Cursor {
            version: 1,
            provider: provider.into(),
            parent: input.parent.clone(),
            query: input.query.clone(),
            next: following,
        })
        .map_err(|_| Error::Unavailable)?;
        let encoded = URL_SAFE_NO_PAD.encode(bytes);
        if encoded.len() > 4096 {
            return Err(Error::Unavailable);
        }
        encoded
    };
    let continue_scan = slack::continue_scan(provider, input, items.len(), &cursor);
    Ok((
        None,
        Some(Page {
            items,
            cursor,
            continue_scan,
        }),
    ))
}

fn plan(provider: &str, parent: &str, query: &str, next: &str) -> Result<Option<Operation>> {
    match provider {
        "slack" => slack::plan(provider, parent, query, next),
        "google_drive" | "gmail" => google::plan(provider, parent, query, next),
        "notion" => notion::plan(provider, parent, query, next),
        "onedrive" | "sharepoint" | "outlook" | "teams" => {
            microsoft::plan(provider, parent, query, next)
        }
        "confluence" | "jira" => atlassian::plan(provider, parent, query, next),
        "box" | "dropbox" | "github" => files::plan(provider, parent, query, next),
        _ => Err(Error::Unavailable),
    }
}

fn interpret(
    provider: &str,
    parent: &str,
    query: &str,
    page_token: &str,
    value: &Value,
) -> Result<(Vec<Resource>, String)> {
    match provider {
        "slack" => slack::interpret(provider, parent, query, page_token, value),
        "google_drive" | "gmail" => google::interpret(provider, parent, query, page_token, value),
        "notion" => notion::interpret(provider, parent, query, page_token, value),
        "onedrive" | "sharepoint" | "outlook" | "teams" => {
            microsoft::interpret(provider, parent, query, page_token, value)
        }
        "confluence" | "jira" => atlassian::interpret(provider, parent, query, page_token, value),
        "box" | "dropbox" | "github" => {
            files::interpret(provider, parent, query, page_token, value)
        }
        _ => Err(Error::Unavailable),
    }
}

#[cfg(test)]
mod tests;
