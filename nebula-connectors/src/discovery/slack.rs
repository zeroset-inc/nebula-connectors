use super::{Error, Operation, Resource, Result, common::*};
use serde_json::{Value, json};

pub(super) fn plan(
    provider: &str,
    parent: &str,
    _query: &str,
    next: &str,
) -> Result<Option<Operation>> {
    let (name, mut args) = match provider {
        "slack" => {
            if !parent.is_empty() {
                return Ok(None);
            }
            (
                "setup.conversations.list",
                json!({"types":"public_channel","exclude_archived":"true","limit":"200"}),
            )
        }
        _ => return Err(Error::Unavailable),
    };
    if !next.is_empty() {
        args["cursor"] = next.into();
    }
    operation(name, args)
}

pub(super) fn interpret(
    provider: &str,
    _parent: &str,
    query: &str,
    _page_token: &str,
    value: &Value,
) -> Result<(Vec<Resource>, String)> {
    let mut items = vec![];
    let next = match provider {
        "slack" => {
            if value["ok"] != true {
                return Err(match text(value, "error") {
                    "invalid_auth" | "not_authed" | "token_revoked" | "account_inactive"
                    | "missing_scope" => Error::Revoked,
                    "ratelimited" => Error::RateLimited,
                    _ => Error::Unavailable,
                });
            }
            let needle = query.trim().strip_prefix('#').unwrap_or(query.trim());
            for row in rows(value, "channels")? {
                let (id, name) = (text(row, "id"), text(row, "name"));
                if !id.is_empty() && id.len() <= 4096 && label_valid(name) && matches(name, needle)
                {
                    items.push(resource(
                        id,
                        &format!("#{name}"),
                        "channel",
                        "",
                        false,
                        true,
                    ));
                }
            }
            text(&value["response_metadata"], "next_cursor").into()
        }
        _ => return Err(Error::Unavailable),
    };
    Ok((items, next))
}

pub(super) fn continue_scan(
    provider: &str,
    input: &super::Request,
    items: usize,
    cursor: &str,
) -> bool {
    provider == "slack"
        && !input
            .query
            .trim()
            .strip_prefix('#')
            .unwrap_or(input.query.trim())
            .is_empty()
        && input.pages + 1 < super::MAX_PAGES
        && input.matched + items < super::SEARCH_MATCHES
        && !cursor.is_empty()
}
