use super::{Error, Operation, Resource, Result, common::*};
use serde_json::{Value, json};

fn cloud_id(id: &str) -> bool {
    id.len() == 36
        && id
            .bytes()
            .all(|b| b.is_ascii_digit() || b"abcdef-".contains(&b))
}
fn atlassian_key(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 255
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_-".contains(&b))
}
pub(super) fn plan(
    provider: &str,
    parent: &str,
    query: &str,
    next: &str,
) -> Result<Option<Operation>> {
    let (name, mut args) = match provider {
        "confluence" | "jira" => {
            if parent.is_empty() {
                return operation("setup.sites.list", json!({}));
            }
            if !cloud_id(parent) {
                return Err(Error::InvalidCursor);
            }
            if provider == "confluence" {
                ("setup.spaces.list", json!({"cloud":parent,"limit":"100"}))
            } else {
                let start = if next.is_empty() { "0" } else { next };
                start.parse::<u64>().map_err(|_| Error::InvalidCursor)?;
                let mut args = json!({"cloud":parent,"maxResults":"50","startAt":start});
                if !query.trim().is_empty() {
                    args["query"] = query.trim().into();
                }
                return operation("setup.projects.search", args);
            }
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
    parent: &str,
    query: &str,
    _page_token: &str,
    value: &Value,
) -> Result<(Vec<Resource>, String)> {
    let mut items = vec![];
    let mut next = String::new();
    match provider {
        "confluence" | "jira" if parent.is_empty() => {
            for row in rows(value, "")? {
                let (id, name) = (text(row, "id"), text(row, "name"));
                let granted = rows(row, "scopes")?
                    .iter()
                    .any(|scope| scope.as_str().is_some_and(|s| s.contains(provider)));
                if cloud_id(id) && label_valid(name) && granted && matches(name, query) {
                    items.push(resource(id, name, "site", "", true, false));
                }
            }
        }
        "confluence" => {
            for row in rows(value, "results")? {
                let (id, name, key) = (text(row, "id"), text(row, "name"), text(row, "key"));
                if atlassian_key(id)
                    && label_valid(name)
                    && matches(&format!("{name} {key}"), query)
                {
                    items.push(resource(
                        &format!("{parent}~{id}"),
                        name,
                        "space",
                        parent,
                        false,
                        true,
                    ));
                }
            }
            let link = text(&value["_links"], "next");
            if !link.is_empty() {
                next = next_query(link, &["cursor"])?
                    .remove("cursor")
                    .unwrap_or_default();
            }
        }
        "jira" => {
            let values = rows(value, "values")?;
            for row in values {
                let (key, name) = (text(row, "key"), text(row, "name"));
                if atlassian_key(key) && label_valid(name) {
                    items.push(resource(
                        &format!("{parent}~{key}"),
                        &format!("{name} ({key})"),
                        "project",
                        parent,
                        false,
                        true,
                    ));
                }
            }
            if value["isLast"] != true && !values.is_empty() {
                next = value["startAt"]
                    .as_u64()
                    .unwrap_or(0)
                    .checked_add(values.len() as u64)
                    .ok_or(Error::Unavailable)?
                    .to_string();
            }
        }
        _ => return Err(Error::Unavailable),
    }
    Ok((items, next))
}
