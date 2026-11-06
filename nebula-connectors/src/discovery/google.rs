use super::{Error, Operation, Resource, Result, common::*};
use serde_json::{Value, json};

pub(super) fn plan(
    provider: &str,
    parent: &str,
    _query: &str,
    next: &str,
) -> Result<Option<Operation>> {
    let (name, mut args) = match provider {
        "google_drive" => {
            let folder = if parent.is_empty() { "root" } else { parent };
            if !crate::google::google_id(folder) {
                return Err(Error::InvalidCursor);
            }
            (
                "setup.folders.list",
                json!({"folder":folder,"fields":"nextPageToken,files(id,name)","orderBy":"name","pageSize":"100"}),
            )
        }
        "gmail" => {
            if !parent.is_empty() {
                return Ok(None);
            }
            if !next.is_empty() {
                return Err(Error::InvalidCursor);
            }
            return operation("setup.labels.list", json!({}));
        }
        _ => return Err(Error::Unavailable),
    };
    if !next.is_empty() {
        args["pageToken"] = next.into();
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
        "google_drive" => {
            for row in rows(value, "files")? {
                let (id, name) = (text(row, "id"), text(row, "name"));
                if crate::google::google_id(id) && label_valid(name) && matches(name, query) {
                    items.push(resource(id, name, "folder", parent, true, true));
                }
            }
            next = text(value, "nextPageToken").into();
            if next.len() > 2048 {
                return Err(Error::Unavailable);
            }
        }
        "gmail" => {
            let mut system = vec![];
            let mut user = vec![];
            for row in rows(value, "labels")? {
                let (id, original) = (text(row, "id"), text(row, "name"));
                if !crate::google::google_id(id) || !label_valid(original) {
                    continue;
                }
                let name = match id {
                    "INBOX" => "Inbox",
                    "SENT" => "Sent",
                    "STARRED" => "Starred",
                    "IMPORTANT" => "Important",
                    _ => original,
                };
                let is_system =
                    name != original || ["INBOX", "SENT", "STARRED", "IMPORTANT"].contains(&id);
                if !matches(name, query) || !is_system && text(row, "type") != "user" {
                    continue;
                }
                let target = if is_system { &mut system } else { &mut user };
                target.push(resource(id, name, "label", "", false, true));
            }
            system.sort_by(|a, b| a.name.cmp(&b.name));
            user.sort_by_key(|r| r.name.to_lowercase());
            system.extend(user);
            items = system;
        }
        _ => return Err(Error::Unavailable),
    }
    Ok((items, next))
}
