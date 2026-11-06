use std::collections::HashSet;

use nebula_connector_sdk::{
    Connector, ConnectorError, Context, Page, Specification,
    connector::{Capabilities, Resource, ResourcePage, stable_id},
    protocol::{ConnectorEvent, ItemMetadata},
    scope::Selection,
};
use serde_json::{Value, json};

use crate::{
    atlassian::{check_account, document_text, split, validate_context},
    common::{Cursor, Response, Result, observed_now, offset_timestamp_ms, request},
};

/// Issues per search. Each costs two events and up to `MAX_TEXT_BYTES`, so
/// four keep a page within the runner's 1 MiB batch.
const PAGE_SIZE: usize = 4;
const MAX_TEXT_BYTES: usize = 96 << 10;
const MAX_COMMENTS: usize = 100;
const REPRESENTATION: &str = "jira-issue-v1";
const CONTENT_TYPE: &str = "application/vnd.atlassian.jira.issue";
const FIELDS: &str = "summary,description,status,issuetype,created,updated,comment";

/// Managed Jira project inventories: every issue in a selected project, as
/// its summary, description and comments. A container names the site and
/// project key, "cloud~KEY".
pub struct Jira;

impl Connector for Jira {
    fn specification(&self) -> Specification {
        Specification {
            provider: "jira".into(),
            version: env!("CARGO_PKG_VERSION").into(),
            cursor_version: 1,
            config_schema: json!({"type":"object","properties":{},"additionalProperties":false}),
            capabilities: Capabilities {
                managed: true,
                incremental: false,
                complete_inventory: true,
                permissions: false,
                original_files: false,
                supported_filters: ["container", "created_at", "modified_at"]
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
        Box::pin(check_account(context))
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
                .and_then(|cursor| cursor["project"].as_u64())
                .unwrap_or_default() as usize;
            let projects = context.scope.containers();
            let container = projects
                .get(position)
                .ok_or(ConnectorError::CursorExpired)?;
            let (cloud, project) = split(container)?;
            let info = request(
                context,
                "projects.get",
                json!({"cloud":cloud,"project":project}),
            )
            .await?;
            let name = info["name"]
                .as_str()
                .filter(|name| !name.is_empty() && name.len() <= 1024)
                .ok_or(ConnectorError::Response)?;
            Ok(ResourcePage {
                resources: vec![Resource {
                    id: container.clone(),
                    name: format!("{name} ({project})"),
                    kind: "project".into(),
                    parent_id: Some(cloud.into()),
                }],
                next: (position + 1 < projects.len()).then(|| json!({"project":position + 1})),
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
    let projects = context.scope.containers();
    let mut cursor = Cursor::parse(value, projects.len())?;
    let Some(container) = projects.get(cursor.container) else {
        return page(cursor, vec![], true);
    };
    let (cloud, project) = split(container)?;
    let mut arguments = json!({"cloud":cloud,"project":project,"fields":FIELDS,
        "maxResults":PAGE_SIZE.to_string()});
    if !cursor.page.is_empty() {
        arguments["nextPageToken"] = cursor.page.clone().into();
    }
    let search = request(context, "issues.search", arguments).await?;
    let issues = search["issues"]
        .as_array()
        .ok_or(ConnectorError::Response)?;
    if issues.len() > PAGE_SIZE {
        return Err(ConnectorError::Response);
    }
    let observed_at_ms = observed_now()?;
    let mut events = Vec::with_capacity(issues.len() * 2);
    let mut identities = HashSet::with_capacity(issues.len());
    for issue in issues {
        let id = issue["id"]
            .as_str()
            .filter(|id| {
                (1..=32).contains(&id.len()) && id.bytes().all(|byte| byte.is_ascii_digit())
            })
            .ok_or(ConnectorError::Response)?;
        let key = issue["key"].as_str().ok_or(ConnectorError::Response)?;
        // The search names the project in its query; each issue is checked
        // against it here.
        if key.split_once('-').map(|(prefix, _)| prefix) != Some(project)
            || !identities.insert(id.to_owned())
        {
            return Err(ConnectorError::Response);
        }
        events.extend(issue_events(
            context,
            container,
            cloud,
            project,
            id,
            key,
            &issue["fields"],
            observed_at_ms,
        )?);
    }
    let next = match &search["nextPageToken"] {
        Value::Null => None,
        token @ Value::String(_) if search["isLast"] != true => Some(token),
        Value::String(_) => None,
        _ => return Err(ConnectorError::Response),
    };
    cursor.advance(next)?;
    let complete = cursor.container == projects.len();
    page(cursor, events, complete)
}

fn page(cursor: Cursor, events: Vec<ConnectorEvent>, complete: bool) -> Result<Page> {
    Ok(Page {
        cursor: serde_json::to_value(cursor).map_err(|_| ConnectorError::Response)?,
        events,
        complete,
    })
}

/// The issue as text: key and summary, its type and status, the description,
/// then each comment with its author and day.
fn issue_text(key: &str, fields: &Value) -> String {
    let summary = fields["summary"].as_str().unwrap_or_default();
    let mut text = format!("{key}: {summary}\n");
    let kind = fields["issuetype"]["name"].as_str().unwrap_or("Issue");
    let status = fields["status"]["name"].as_str().unwrap_or("Unknown");
    text.push_str(&format!("Type: {kind} · Status: {status}\n"));
    let description = document_text(&fields["description"]);
    if !description.is_empty() {
        text.push('\n');
        text.push_str(&description);
        text.push('\n');
    }
    let comments = fields["comment"]["comments"]
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or_default();
    if !comments.is_empty() {
        text.push_str("\nComments\n");
    }
    for comment in comments.iter().take(MAX_COMMENTS) {
        let author = comment["author"]["displayName"]
            .as_str()
            .unwrap_or("Someone");
        let day = comment["created"]
            .as_str()
            .and_then(|created| created.get(..10))
            .unwrap_or_default();
        text.push_str(&format!(
            "\n{author} ({day}):\n{}\n",
            document_text(&comment["body"])
        ));
    }
    text
}

#[allow(clippy::too_many_arguments)]
fn issue_events(
    context: &Context,
    container: &str,
    cloud: &str,
    project: &str,
    id: &str,
    key: &str,
    fields: &Value,
    observed_at_ms: i64,
) -> Result<Vec<ConnectorEvent>> {
    let object = format!("{cloud}/{id}");
    let updated = fields["updated"].as_str().ok_or(ConnectorError::Response)?;
    let metadata = json!({"container_id":container,"path":format!("{container}/{key}"),"content_type":CONTENT_TYPE,
        "created_at_ms":offset_timestamp_ms(fields["created"].as_str().ok_or(ConnectorError::Response)?)?,
        "modified_at_ms":offset_timestamp_ms(updated)?});
    let text = issue_text(key, fields);
    let fits = text.len() <= MAX_TEXT_BYTES;
    let revision = stable_id(REPRESENTATION, &[object.as_bytes(), updated.as_bytes()]);
    let source = json!({"authority":context.authority,"object_id":object,"revision":revision});
    let metadata: ItemMetadata =
        serde_json::from_value(metadata).map_err(|_| ConnectorError::Response)?;
    let selection = context.scope.select(&metadata);
    let observation_id = stable_id(
        "jira-observation-v1",
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
    let (outcome, reason) = match selection {
        Selection::Excluded => ("skipped", selection.reason()),
        Selection::MissingMetadata => ("failed", selection.reason()),
        _ if fits => ("fetched", "in_scope"),
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
        let mut change = json!({"type":kind,"change_id":stable_id("jira-change-v1", &[observation_id.as_bytes(),kind.as_bytes()]),
            "observation_id":observation_id,"source":source,"observed_at_ms":observed_at_ms});
        if outcome == "fetched" {
            let summary: String = fields["summary"]
                .as_str()
                .unwrap_or_default()
                .chars()
                .take(1024)
                .collect();
            change["permissions"] = json!({"state":"unavailable"});
            change["metadata"] = json!({"provider":"jira","issue_id":id,"issue_key":key,"project":project,"title":summary});
            change["content"] = json!({"kind":"text","text":text,"media_type":"text/plain",
                "representation_version":REPRESENTATION});
        }
        events.push(change);
    }
    events
        .into_iter()
        .map(|event| serde_json::from_value(event).map_err(|_| ConnectorError::Response))
        .collect()
}
