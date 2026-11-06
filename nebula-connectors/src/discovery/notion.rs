use super::{Error, Operation, Resource, Result, common::*};
use serde_json::{Value, json};

fn notion_id(id: &str) -> bool {
    let mut digits = 0;
    for (index, b) in id.bytes().enumerate() {
        if b == b'-' {
            if ![8, 12, 16, 20].contains(&digits) || index > 0 && id.as_bytes()[index - 1] == b'-' {
                return false;
            }
        } else if b.is_ascii_digit() || b"abcdef".contains(&b) {
            digits += 1;
        } else {
            return false;
        }
    }
    digits == 32
}
pub(super) fn plan(
    provider: &str,
    parent: &str,
    query: &str,
    next: &str,
) -> Result<Option<Operation>> {
    match provider {
        "notion" => {
            if !parent.is_empty() {
                return Ok(None);
            }
            let mut search = json!({"filter":{"property":"object","value":"page"},"page_size":100});
            if !query.trim().is_empty() {
                search["query"] = query.trim().into();
            }
            if !next.is_empty() {
                search["start_cursor"] = next.into();
            }
            operation("setup.search", json!({"search":search}))
        }
        _ => Err(Error::Unavailable),
    }
}

pub(super) fn interpret(
    provider: &str,
    _parent: &str,
    _query: &str,
    _page_token: &str,
    value: &Value,
) -> Result<(Vec<Resource>, String)> {
    let mut items = vec![];
    let mut next = String::new();
    match provider {
        "notion" => {
            for row in rows(value, "results")? {
                let id = text(row, "id");
                if text(row, "object") != "page" || row["archived"] == true || !notion_id(id) {
                    continue;
                }
                let title = row["properties"]
                    .as_object()
                    .and_then(|properties| {
                        properties.values().find_map(|property| {
                            if text(property, "type") != "title" {
                                return None;
                            }
                            let title = property["title"]
                                .as_array()?
                                .iter()
                                .map(|part| text(part, "plain_text"))
                                .collect::<String>();
                            (title.trim().len() <= 1024).then(|| title.trim().to_owned())
                        })
                    })
                    .unwrap_or_default();
                items.push(resource(
                    id,
                    if title.is_empty() { "Untitled" } else { &title },
                    "page",
                    "",
                    false,
                    true,
                ));
            }
            if text(value, "next_cursor").len() > 1024 {
                return Err(Error::Unavailable);
            }
            if value["has_more"] == true {
                next = text(value, "next_cursor").into();
            }
        }
        _ => return Err(Error::Unavailable),
    }
    Ok((items, next))
}
