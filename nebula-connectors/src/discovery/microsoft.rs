use super::{Error, Operation, Resource, Result, common::*};
use serde_json::{Value, json};

fn graph_id(id: &str) -> bool {
    id.len() <= 512 && !id.contains('~') && crate::graph::graph_id(id)
}
pub(super) fn plan(
    provider: &str,
    parent: &str,
    query: &str,
    next: &str,
) -> Result<Option<Operation>> {
    let (name, mut args) = match provider {
        "onedrive" => {
            if parent.is_empty() {
                (
                    "setup.root.children",
                    json!({"select":"id,name,folder","top":"200"}),
                )
            } else {
                if !graph_id(parent) {
                    return Err(Error::InvalidCursor);
                }
                (
                    "setup.items.children",
                    json!({"item":parent,"select":"id,name,folder","top":"200"}),
                )
            }
        }
        "sharepoint" => {
            if parent.is_empty() {
                (
                    "setup.sites.search",
                    json!({"search":if query.trim().is_empty() { "*" } else { query.trim() },"select":"id,displayName,name","top":"100"}),
                )
            } else {
                if !graph_id(parent) {
                    return Err(Error::InvalidCursor);
                }
                (
                    "setup.drives.list",
                    json!({"site":parent,"select":"id,name"}),
                )
            }
        }
        "outlook" => {
            if !parent.is_empty() {
                return Ok(None);
            }
            (
                "setup.folders.list",
                json!({"select":"id,displayName,childFolderCount","top":"100"}),
            )
        }
        "teams" => {
            if parent.is_empty() {
                ("setup.teams.list", json!({"select":"id,displayName"}))
            } else {
                if !graph_id(parent) {
                    return Err(Error::InvalidCursor);
                }
                (
                    "setup.channels.list",
                    json!({"team":parent,"select":"id,displayName"}),
                )
            }
        }
        _ => return Err(Error::Unavailable),
    };
    if !next.is_empty() {
        let paging: std::collections::BTreeMap<String, String> =
            serde_json::from_str(next).map_err(|_| Error::InvalidCursor)?;
        if paging.is_empty() || paging.len() > 2 {
            return Err(Error::InvalidCursor);
        }
        for (key, value) in paging {
            if !["skip", "skiptoken"].contains(&key.as_str())
                || value.is_empty()
                || value.len() > 4096
            {
                return Err(Error::InvalidCursor);
            }
            args[key] = value.into();
        }
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
        "onedrive" | "sharepoint" | "outlook" | "teams" => {
            for row in rows(value, "value")? {
                let id = text(row, "id");
                let label = if text(row, "displayName").is_empty() {
                    text(row, "name")
                } else {
                    text(row, "displayName")
                };
                if !graph_id(id) || !label_valid(label) {
                    continue;
                }
                let item = match provider {
                    "onedrive"
                        if row["folder"].is_object() && matches(text(row, "name"), query) =>
                    {
                        Some(resource(
                            id,
                            text(row, "name"),
                            "folder",
                            parent,
                            row["folder"]["childCount"].as_u64().unwrap_or(0) > 0,
                            true,
                        ))
                    }
                    "sharepoint" if parent.is_empty() => {
                        Some(resource(id, label, "site", "", true, false))
                    }
                    "sharepoint" if matches(text(row, "name"), query) => Some(resource(
                        id,
                        text(row, "name"),
                        "library",
                        parent,
                        false,
                        true,
                    )),
                    "outlook" if matches(text(row, "displayName"), query) => Some(resource(
                        id,
                        text(row, "displayName"),
                        "folder",
                        "",
                        false,
                        true,
                    )),
                    "teams" if matches(text(row, "displayName"), query) && parent.is_empty() => {
                        Some(resource(
                            id,
                            text(row, "displayName"),
                            "team",
                            "",
                            true,
                            false,
                        ))
                    }
                    "teams" if matches(text(row, "displayName"), query) => Some(resource(
                        &format!("{parent}~{id}"),
                        &format!("#{}", text(row, "displayName")),
                        "channel",
                        parent,
                        false,
                        true,
                    )),
                    _ => None,
                };
                if let Some(item) = item {
                    items.push(item);
                }
            }
            let link = text(value, "@odata.nextLink");
            if !link.is_empty() {
                let paging = next_query(link, &["$skiptoken", "$skip"])?;
                if paging.is_empty() {
                    return Err(Error::Unavailable);
                }
                next = serde_json::to_string(&paging).map_err(|_| Error::Unavailable)?;
            }
        }
        _ => return Err(Error::Unavailable),
    }
    Ok((items, next))
}
