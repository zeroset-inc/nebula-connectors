use std::collections::HashSet;

use nebula_connector_sdk::{
    Connector, ConnectorError, Context, Page, Specification,
    client::CredentialMode,
    connector::{Capabilities, Resource, ResourcePage, stable_id},
    protocol::{ConnectorEvent, ItemMetadata},
    scope::Selection,
};
use serde_json::{Value, json};

use crate::common::{Cursor, Response, Result, observed_now, request, timestamp_ms};

/// Issues per page. Each costs two events and up to `MAX_TEXT_BYTES`, so
/// four keep a page within the runner's 1 MiB batch.
const PAGE_SIZE: usize = 4;
const MAX_TEXT_BYTES: usize = 96 << 10;
const COMMENTS: &str = "20";
const REPRESENTATION: &str = "github-issue-v1";

/// Splits an "owner/name" repository container.
fn split(container: &str) -> Result<(&str, &str)> {
    let (owner, repository) = container
        .split_once('/')
        .ok_or(ConnectorError::Configuration)?;
    let owner_ok = (1..=39).contains(&owner.len())
        && owner
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-');
    let repository_ok = (1..=100).contains(&repository.len())
        && repository != "."
        && repository != ".."
        && repository
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"_.-".contains(&byte));
    if owner_ok && repository_ok {
        Ok((owner, repository))
    } else {
        Err(ConnectorError::Configuration)
    }
}

/// Managed GitHub repository inventories: every issue and pull request in a
/// selected repository, as Markdown with its comments. A container names the
/// repository, "owner/name"; the authority is the user's numeric ID.
pub struct GitHub;

fn validate_context(context: &Context) -> Result<()> {
    if context.credential_mode != CredentialMode::Managed
        || !context.complete_inventory
        || context.config != json!({})
        || !(1..=20).contains(&context.authority.len())
        || !context.authority.bytes().all(|byte| byte.is_ascii_digit())
        || context
            .scope
            .containers()
            .iter()
            .any(|container| split(container).is_err())
    {
        return Err(ConnectorError::Configuration);
    }
    Ok(())
}

impl Connector for GitHub {
    fn specification(&self) -> Specification {
        Specification {
            provider: "github".into(),
            version: env!("CARGO_PKG_VERSION").into(),
            cursor_version: 1,
            config_schema: json!({"type":"object","properties":{},"additionalProperties":false}),
            capabilities: Capabilities {
                managed: true,
                incremental: false,
                complete_inventory: true,
                permissions: false,
                original_files: false,
                supported_filters: ["container", "content_type", "created_at", "modified_at"]
                    .map(str::to_owned)
                    .into(),
            },
        }
    }

    fn check<'life0, 'life1, 'async_trait>(
        &'life0 self,
        context: &'life1 Context,
    ) -> Response<'async_trait, ()>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            validate_context(context)?;
            let user = request(context, "user.get", json!({})).await?;
            if user["id"].as_u64().map(|id| id.to_string()).as_deref()
                != Some(context.authority.as_str())
            {
                return Err(ConnectorError::Permission);
            }
            Ok(())
        })
    }

    fn discover<'life0, 'life1, 'life2, 'async_trait>(
        &'life0 self,
        context: &'life1 Context,
        cursor: Option<&'life2 Value>,
    ) -> Response<'async_trait, ResourcePage>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            validate_context(context)?;
            let position = cursor
                .and_then(|cursor| cursor["repository"].as_u64())
                .unwrap_or_default() as usize;
            let repositories = context.scope.containers();
            let container = repositories
                .get(position)
                .ok_or(ConnectorError::CursorExpired)?;
            let (owner, repository) = split(container)?;
            let info = request(
                context,
                "repos.get",
                json!({"owner":owner,"repo":repository}),
            )
            .await?;
            let name = info["full_name"]
                .as_str()
                .filter(|name| name.eq_ignore_ascii_case(container))
                .ok_or(ConnectorError::Response)?;
            Ok(ResourcePage {
                resources: vec![Resource {
                    id: container.clone(),
                    name: name.into(),
                    kind: "repository".into(),
                    parent_id: None,
                }],
                next: (position + 1 < repositories.len())
                    .then(|| json!({"repository":position + 1})),
            })
        })
    }

    fn read<'life0, 'life1, 'life2, 'async_trait>(
        &'life0 self,
        context: &'life1 Context,
        cursor: &'life2 Value,
    ) -> Response<'async_trait, Page>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(read(context, cursor))
    }
}

async fn read(context: &Context, value: &Value) -> Result<Page> {
    validate_context(context)?;
    let repositories = context.scope.containers();
    let mut cursor = Cursor::parse(value, repositories.len())?;
    let Some(container) = repositories.get(cursor.container) else {
        return page(cursor, vec![], true);
    };
    let (owner, repository) = split(container)?;
    // Oldest first, so issues opened during a scan land on later pages.
    let number = if cursor.page.is_empty() {
        1
    } else {
        cursor
            .page
            .parse::<u32>()
            .ok()
            .filter(|number| (2..=100_000).contains(number))
            .ok_or(ConnectorError::CursorExpired)?
    };
    let issues = request(
        context,
        "issues.list",
        json!({"owner":owner,"repo":repository,"state":"all","sort":"created","direction":"asc",
            "per_page":PAGE_SIZE.to_string(),"page":number.to_string()}),
    )
    .await?;
    let issues = issues.as_array().ok_or(ConnectorError::Response)?;
    if issues.len() > PAGE_SIZE {
        return Err(ConnectorError::Response);
    }
    let observed_at_ms = observed_now()?;
    let mut events = Vec::with_capacity(issues.len() * 2);
    let mut identities = HashSet::with_capacity(issues.len());
    for issue in issues {
        let id = issue["id"].as_u64().ok_or(ConnectorError::Response)?;
        if !identities.insert(id) {
            return Err(ConnectorError::Response);
        }
        events.extend(
            issue_events(
                context,
                container,
                owner,
                repository,
                id,
                issue,
                observed_at_ms,
            )
            .await?,
        );
    }
    let next = (issues.len() == PAGE_SIZE).then(|| json!((number + 1).to_string()));
    cursor.advance(next.as_ref())?;
    let complete = cursor.container == repositories.len();
    page(cursor, events, complete)
}

fn page(cursor: Cursor, events: Vec<ConnectorEvent>, complete: bool) -> Result<Page> {
    Ok(Page {
        cursor: serde_json::to_value(cursor).map_err(|_| ConnectorError::Response)?,
        events,
        complete,
    })
}

/// The issue or pull request as Markdown: number and title, its kind, state
/// and labels, the body, then each comment with its author and day.
fn issue_text(issue: &Value, comments: &[Value]) -> String {
    let number = issue["number"].as_u64().unwrap_or_default();
    let title = issue["title"].as_str().unwrap_or_default();
    let kind = if issue
        .get("pull_request")
        .is_some_and(|pull| !pull.is_null())
    {
        "Pull request"
    } else {
        "Issue"
    };
    let state = issue["state"].as_str().unwrap_or("unknown");
    let mut text = format!("# #{number} {title}\n\n{kind} · {state}");
    let labels: Vec<&str> = issue["labels"]
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or_default()
        .iter()
        .filter_map(|label| label["name"].as_str())
        .collect();
    if !labels.is_empty() {
        text.push_str(&format!(" · labels: {}", labels.join(", ")));
    }
    if let Some(author) = issue["user"]["login"].as_str() {
        text.push_str(&format!(" · opened by {author}"));
    }
    text.push('\n');
    if let Some(body) = issue["body"]
        .as_str()
        .filter(|body| !body.trim().is_empty())
    {
        text.push('\n');
        text.push_str(body.trim_end());
        text.push('\n');
    }
    if !comments.is_empty() {
        text.push_str("\n## Comments\n");
    }
    for comment in comments {
        let author = comment["user"]["login"].as_str().unwrap_or("someone");
        let day = comment["created_at"]
            .as_str()
            .and_then(|created| created.get(..10))
            .unwrap_or_default();
        let body = comment["body"].as_str().unwrap_or_default().trim_end();
        text.push_str(&format!("\n**{author}** ({day}):\n\n{body}\n"));
    }
    text
}

async fn issue_events(
    context: &Context,
    container: &str,
    owner: &str,
    repository: &str,
    id: u64,
    issue: &Value,
    observed_at_ms: i64,
) -> Result<Vec<ConnectorEvent>> {
    let number = issue["number"].as_u64().ok_or(ConnectorError::Response)?;
    let object = id.to_string();
    let updated = issue["updated_at"]
        .as_str()
        .ok_or(ConnectorError::Response)?;
    let pull = issue
        .get("pull_request")
        .is_some_and(|pull| !pull.is_null());
    let content_type = if pull {
        "application/vnd.github.pull-request"
    } else {
        "application/vnd.github.issue"
    };
    let metadata = json!({"container_id":container,"path":format!("{container}#{number}"),"content_type":content_type,
        "created_at_ms":timestamp_ms(issue["created_at"].as_str().ok_or(ConnectorError::Response)?)?,
        "modified_at_ms":timestamp_ms(updated)?});
    // A new comment moves the issue's updated time, so it starts a revision.
    let revision = stable_id(REPRESENTATION, &[object.as_bytes(), updated.as_bytes()]);
    let mut source = json!({"authority":context.authority,"object_id":object,"revision":revision});
    if let Some(link) = issue["html_url"]
        .as_str()
        .filter(|link| link.starts_with("https://") && link.len() <= 8192)
    {
        source["url"] = link.into();
    }
    let metadata: ItemMetadata =
        serde_json::from_value(metadata).map_err(|_| ConnectorError::Response)?;
    let selection = context.scope.select(&metadata);
    let observation_id = stable_id(
        "github-observation-v1",
        &[
            context.run.scan_id.as_bytes(),
            context.run.id.as_bytes(),
            &context.run.fence.to_be_bytes(),
            &context.run.next_sequence.to_be_bytes(),
            context.authority.as_bytes(),
            object.as_bytes(),
            revision.as_bytes(),
        ],
    );
    // Comments are read only for selected issues, so an exclusion costs one
    // listing entry.
    let mut text = None;
    if selection == Selection::Selected {
        let comments = if issue["comments"].as_u64().unwrap_or_default() > 0 {
            request(
                context,
                "issues.comments",
                json!({"owner":owner,"repo":repository,"number":number.to_string(),"per_page":COMMENTS}),
            )
            .await?
        } else {
            json!([])
        };
        let comments = comments.as_array().ok_or(ConnectorError::Response)?;
        text = Some(issue_text(issue, comments));
    }
    let (outcome, reason) = match (selection, &text) {
        (Selection::Excluded, _) => ("skipped", selection.reason()),
        (Selection::MissingMetadata, _) => ("failed", selection.reason()),
        (_, Some(text)) if text.len() <= MAX_TEXT_BYTES => ("fetched", "in_scope"),
        _ => ("skipped", "unsupported"),
    };
    let mut events = vec![
        json!({"type":"item_observed","observation_id":observation_id,
        "scan_id":context.run.scan_id,"source":source,"observed_at_ms":observed_at_ms,
        "metadata":metadata,"selection":selection.selection(),"outcome":outcome,"reason":reason}),
    ];
    if selection == Selection::Selected {
        let kind = if outcome == "fetched" {
            "upsert"
        } else {
            "presence"
        };
        let mut change = json!({"type":kind,"change_id":stable_id("github-change-v1", &[observation_id.as_bytes(),kind.as_bytes()]),
            "observation_id":observation_id,"source":source,"observed_at_ms":observed_at_ms});
        if let (Some(text), "fetched") = (text, outcome) {
            let title: String = issue["title"]
                .as_str()
                .unwrap_or_default()
                .chars()
                .take(1024)
                .collect();
            change["permissions"] = json!({"state":"unavailable"});
            change["metadata"] = json!({"provider":"github","repository":container,"number":number,
                "pull_request":pull,"title":title});
            change["content"] = json!({"kind":"text","text":text,"media_type":"text/markdown",
                "representation_version":REPRESENTATION});
        }
        events.push(change);
    }
    events
        .into_iter()
        .map(|event| serde_json::from_value(event).map_err(|_| ConnectorError::Response))
        .collect()
}
