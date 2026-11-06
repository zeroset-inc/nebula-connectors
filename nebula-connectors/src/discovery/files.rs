use super::{Error, Operation, Resource, Result, common::*};
use serde_json::{Value, json};

fn box_id(id: &str) -> bool {
    !id.is_empty() && id.len() <= 32 && id.bytes().all(|b| b.is_ascii_digit())
}
fn dropbox_id(id: &str) -> bool {
    id.strip_prefix("id:").is_some_and(|id| {
        !id.is_empty()
            && id.len() <= 128
            && id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"_-".contains(&b))
    })
}
fn repository(id: &str) -> bool {
    id.split_once('/').is_some_and(|(owner, name)| {
        !owner.is_empty()
            && owner.len() <= 39
            && owner
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-')
            && !name.is_empty()
            && name.len() <= 100
            && name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"_.-".contains(&b))
    })
}
pub(super) fn plan(
    provider: &str,
    parent: &str,
    _query: &str,
    next: &str,
) -> Result<Option<Operation>> {
    match provider {
        "box" => {
            let folder = if parent.is_empty() { "0" } else { parent };
            if !box_id(folder) {
                return Err(Error::InvalidCursor);
            }
            let offset = if next.is_empty() { "0" } else { next };
            offset.parse::<u64>().map_err(|_| Error::InvalidCursor)?;
            operation(
                "setup.folders.list",
                json!({"folder":folder,"fields":"id,type,name","limit":"1000","offset":offset}),
            )
        }
        "dropbox" => {
            if !parent.is_empty() && !dropbox_id(parent) {
                return Err(Error::InvalidCursor);
            }
            if !next.is_empty() {
                operation("setup.folders.continue", json!({"cursor":next}))
            } else if parent.is_empty() {
                operation("setup.root.list", json!({}))
            } else {
                operation("setup.folders.list", json!({"folder":parent}))
            }
        }
        "github" => {
            if !parent.is_empty() {
                return Ok(None);
            }
            let page = if next.is_empty() { "1" } else { next };
            if !page.parse::<usize>().is_ok_and(|n| (1..=1000).contains(&n)) {
                return Err(Error::InvalidCursor);
            }
            operation(
                "setup.repos.list",
                json!({"sort":"full_name","per_page":"100","page":page}),
            )
        }
        _ => Err(Error::Unavailable),
    }
}

pub(super) fn interpret(
    provider: &str,
    parent: &str,
    query: &str,
    page_token: &str,
    value: &Value,
) -> Result<(Vec<Resource>, String)> {
    let mut items = vec![];
    let mut next = String::new();
    match provider {
        "box" | "dropbox" => {
            let entries = rows(value, "entries")?;
            for row in entries {
                let (id, name) = (text(row, "id"), text(row, "name"));
                let valid = if provider == "box" {
                    text(row, "type") == "folder" && box_id(id)
                } else {
                    text(row, ".tag") == "folder" && dropbox_id(id)
                };
                if valid && label_valid(name) && matches(name, query) {
                    items.push(resource(id, name, "folder", parent, true, true));
                }
            }
            if provider == "box" {
                let following = value["offset"]
                    .as_u64()
                    .unwrap_or(0)
                    .checked_add(entries.len() as u64)
                    .ok_or(Error::Unavailable)?;
                if !entries.is_empty() && following < value["total_count"].as_u64().unwrap_or(0) {
                    next = following.to_string();
                }
            } else if value["has_more"] == true {
                next = text(value, "cursor").into();
                if next.len() > 2048 {
                    return Err(Error::Unavailable);
                }
            }
        }
        "github" => {
            let repositories = rows(value, "")?;
            for row in repositories {
                let name = text(row, "full_name");
                if repository(name) && row["archived"] != true && matches(name, query) {
                    items.push(resource(name, name, "repository", "", false, true));
                }
            }
            if repositories.len() == 100 {
                next = page_token
                    .parse::<usize>()
                    .unwrap_or(1)
                    .checked_add(1)
                    .ok_or(Error::Unavailable)?
                    .to_string();
            }
        }
        _ => return Err(Error::Unavailable),
    }
    Ok((items, next))
}
