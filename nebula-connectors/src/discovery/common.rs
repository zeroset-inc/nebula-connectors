use super::{Error, Operation, Resource, Result};
use serde_json::Value;
pub(super) fn operation(name: &'static str, arguments: Value) -> Result<Option<Operation>> {
    Ok(Some(Operation { name, arguments }))
}
pub(super) fn text<'a>(value: &'a Value, key: &str) -> &'a str {
    value[key].as_str().unwrap_or_default()
}
pub(super) fn rows<'a>(value: &'a Value, key: &str) -> Result<&'a [Value]> {
    let field = if key.is_empty() { value } else { &value[key] };
    match field {
        Value::Null => Ok(&[]),
        Value::Array(rows) => Ok(rows),
        _ => Err(Error::Unavailable),
    }
}
pub(super) fn matches(name: &str, query: &str) -> bool {
    name.to_lowercase().contains(&query.trim().to_lowercase())
}
pub(super) fn resource(
    id: &str,
    name: &str,
    kind: &'static str,
    parent: &str,
    children: bool,
    selectable: bool,
) -> Resource {
    Resource {
        id: id.into(),
        name: name.into(),
        kind,
        parent_id: (!parent.is_empty()).then(|| parent.into()),
        has_children: children,
        selectable,
    }
}
pub(super) fn label_valid(name: &str) -> bool {
    !name.is_empty() && name.len() <= 1024
}
pub(super) fn next_query(
    link: &str,
    keys: &[&str],
) -> Result<std::collections::BTreeMap<String, String>> {
    if link.len() > 4096 {
        return Err(Error::Unavailable);
    }
    let url = reqwest::Url::parse("https://provider.invalid/")
        .unwrap()
        .join(link)
        .map_err(|_| Error::Unavailable)?;
    let mut result = std::collections::BTreeMap::new();
    for (key, value) in url.query_pairs() {
        if keys.contains(&key.as_ref())
            && (value.is_empty()
                || value.len() > 4096
                || result
                    .insert(key.trim_start_matches('$').into(), value.into_owned())
                    .is_some())
        {
            return Err(Error::Unavailable);
        }
    }
    Ok(result)
}
