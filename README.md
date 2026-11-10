# Nebula connectors

Apache-2.0 connector protocol, Rust SDK, provider adapters and shared control runtime.
Connectors may use any language; the JSON Schema and shared fixtures define the wire contract.

This is a foundation preview. The Slack adapter supports selected-channel inventories;
file transfer, incremental synchronization and the remaining provider ports are incomplete.
Publishing this package does not activate a provider or deploy a Nebula service.

`nebula-connector-sdk/schema/protocol-v1.json` is the canonical JSON Schema for
connector batches. `fixtures/protocol-v1.json` contains valid and invalid wire
messages for every SDK and third-party implementation. Protocol version, provider
configuration and cursor versions evolve independently.

## Conformance

Run the schema and fixture checks from the repository root:

```sh
uv run --with jsonschema==4.25.1 python -m unittest discover \
  -s nebula-connector-sdk/tests -v
```

Run Rust checks from this repository:

```sh
python3 check.py
cargo fmt --all --check
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo test --locked --workspace
```

CI runs these checks on Linux x86_64 and ARM64, including Python schema conformance.

Rust tests exercise generated decoding against shared fixtures, runtime integrity
and byte bounds, and the real HTTP client against a local stub service. Client
tests cover provider backoff and reserved lease capacity during slow reads;
they do not exercise live providers or the engine.

## Runner

Call `nebula_connector_sdk::runner::run(adapter, client, run_id, scan_id, complete_inventory)`
with an adapter implementing `Connector`. It loads connection configuration, checks
capabilities/configuration and passes explicit scan mode, scope and authority to the
adapter. One page stays in memory until its batch/checkpoint is accepted. Identical
batch retries never re-read the page; restart with the same run ID recovers the
accepted cursor or completed result. After lease expiry, use a new run ID with the
same scan ID to resume that inventory. Cursor-version mismatch requires migration
or a fresh complete scan.

`Context.scope` is a validated `scope::Filter`. Use `select` on item metadata before
downloading; its selection/reason distinguishes exclusions from missing metadata.
Go admission rechecks the same rules. Both implementations consume
`fixtures/scope-v1.json`, including half-open time ranges and recurring-sync limits.

The runner reserves two events for checkpoint/seal, allowing 98 adapter events,
1 MiB batches and 64 KiB versioned cursors. Each invocation allows 10,000 pages and
one hour. Lease renewal runs during provider reads, using database-reported duration
and a monotonic deadline; transient retries stay within that deadline. Cancellation
stops renewal and pending reads. Full inventories alone emit `snapshot_complete`;
incremental completion cannot authorize absence reconciliation.

Runner component tests use the real HTTP SDK with stub adapters/service responses
to cover lost replies, renewal overload/fencing, completion races and restart.
Go HTTP/PostgreSQL tests cover durable completion and stale-owner rejection. The Rust cases run here; Nebula service tests run in its own repository.

## Provider runner

A customer runner pairs once, then serves its explicitly bound connections:

```sh
nebula-connector pair --endpoint https://connectors.example.com
nebula-connector serve
```

Approve the printed code in connection setup. Other connections in the same tenant
can select that runner without pairing again. One process owns secret rotation;
each replacement secret is persisted before exchange. Credentials are atomically
saved in `~/.nebula-connector/runner.json` with owner-only permissions. Both commands
accept `--state-file` for a private directory. `serve` resumes after restart, handles
up to four independent assignments, and renews each run through the SDK. Hosted
tasks retain the one-assignment `run` command below.

The device poll supplies a locally persisted `secret`; matching delivery retries
return the same runner ID until pairing expires or the runner is revoked. Token
requests supply their persisted `replacement_secret` at token exchange. Matching
retries keep the same access token and expiry; a superseded exchange returns zero
remaining seconds and cannot replace a newer credential. Work polls send
`protocol_version: 1`, supported providers, and free assignment slots (`limit` 0–4).
A zero-slot poll maintains liveness without claiming work. Revoking the runner stops
all bindings; removing one binding leaves its other connections intact.

`nebula-connector slack spec` prints capabilities. `nebula-connector slack run`
reads a private JSON file named by `CONNECTOR_RUN_FILE`, containing `endpoint`,
`workspace_id`, `collection_id`, `connection_id`, `run_id`, `scan_id`,
`complete_inventory` and a Nebula `bearer`. Provider credentials stay in the
broker. Output contains only completion identity; interruption leaves the accepted
checkpoint recoverable. Build with `cargo build --locked --release --bins`.

The Slack port currently supports managed, complete inventories of selected
public/private channels, with one structured source per message/reply. Discovery
reads those selected channels only. Configuration is `{}`; authority is the Slack
team ID. Pages contain at most 15 messages and retain at most 15 pending thread IDs.
Content type is `application/vnd.slack.message+json`; created/modified filters use
message/edit timestamps. The parent's revision stays stable when replies arrive.
Provider retention limits, failed threads and broken cursors prevent scan sealing.
Empty messages, file-bearing messages and representations over 48 KiB produce
explicit skipped observations and presence. File transfer, incremental changes,
permission mirroring and self-managed transport remain in progress.

`nebula-connectors/policies/slack-v1.json` permits four read operations through the
broker, binding channel arguments to selected containers. History/replies default
to one request/minute for new unlisted commercial installs; higher tiers need a
separately qualified policy. The runner honors broker/provider backoff with three
fresh attempts and at most 120 seconds per wait; lease loss cancels the wait.
The artifact is not activated by default. OAuth provisioning, download handles,
provider qualification and engine settlement are still required before rollout.

Slack component tests exercise the real adapter, SDK and executable against an
HTTP service stub; Go tests check its actual policy and PostgreSQL/HTTP quota
admission. This repository runs adapter tests; the Nebula service runs its PostgreSQL suite. These are not live Slack or complete ingestion tests.

The Google Drive port (`nebula-connector google_drive run`) supports managed,
complete inventories of the files directly inside each selected folder;
subfolders are selected separately. Configuration is `{}`; authority is the
Google account's numeric ID. Pages hold at most two files. Google Docs and
Slides export as `text/plain`, Sheets as CSV, and plain, Markdown and HTML files
are read as they are, up to 192 KiB each, as text content. Other files, larger
files and empty text produce explicit skipped observations and presence; a file
that cannot be read fails alone, while losing the folder fails the run. A read
file's source revision is a digest of its text, so renaming, moving or sharing
it keeps the revision; other files' revisions follow Drive's file version.

`nebula-connectors/policies/google-drive-v1.json` permits five read operations.
Listings bind the folder to a selected container through a query template, so
the runner cannot list other folders. Exports and downloads name a file ID that
the broker records but cannot yet trace to a selected folder; that check, file
transfer and incremental changes remain in progress. Drive component tests use
the same HTTP service stub as Slack's; they are not live Drive tests.

The Gmail port (`nebula-connector gmail run`) inventories every message carrying
a selected label, four per page. Text is the plain body, or the HTML body when
there is no plain one, behind the subject, sender, recipients and date, up to
96 KiB. Attachments are never read; empty and larger bodies are skipped with
presence, and a message that cannot be read fails alone. The source revision is
the message ID, since Gmail messages do not change. Authority is the Google
account's numeric ID, as for Drive.

`nebula-connectors/policies/gmail-v1.json` permits four read operations. Label
listings bind the label to a selected container; message reads name a message
ID that the broker records but cannot yet trace to a selected label. Gmail
component tests use the same HTTP service stub; they are not live Gmail tests.

The Microsoft 365 ports read through Microsoft Graph for a work account,
whose authority is its Entra object ID. OneDrive inventories the files directly
inside each selected folder and SharePoint the top of each selected document
library: plain, Markdown, CSV and HTML files are read as text up to 192 KiB,
through Graph's download redirect, which the broker follows without the
credential to the catalog's Microsoft hosts; Office and other files are skipped
with presence until file transfer exists. Outlook reads every message in a
selected mail folder as its plain text body behind its headers, up to 96 KiB.
Teams reads a selected channel, named `team~channel`, one message at a time and
then its replies; deleted messages are left out so the inventory withdraws them.
Every run first checks that the token still belongs to the connection's account.

`onedrive-v1.json`, `sharepoint-v1.json`, `outlook-v1.json` and `teams-v1.json`
bind every listing to a selected container; Teams binds the team and channel
together through a container template. Outlook asks Graph for text bodies with
a `Prefer` header. Microsoft component tests use the same HTTP service stub;
they are not live Graph tests.

The Notion port reads each selected page as one Markdown source built from its
blocks: headings, lists, to-dos, quotes, code, tables and nested blocks, up to
192 KiB and 60 block reads. Subpages and databases appear as links and sync only
when selected; archived pages are left out so the inventory withdraws them.
Authority is the Notion workspace ID. `notion-v1.json` binds page reads to
selected pages; block reads name a block the broker records but cannot yet trace
to a selected page.

The Confluence and Jira ports reach a site through `api.atlassian.com` with its
cloud ID, so a container names the site and a space ID or project key:
`<cloud>~<space>` or `<cloud>~<KEY>`. Authority is the Atlassian account ID,
which each run checks against `/me`. Confluence lists a space's current pages
four at a time and reads each one's stored XHTML, up to 96 KiB, as `text/html`;
a page the account cannot open fails alone, and a page read must name the
selected space. Jira searches a project's issues four at a time and writes each
as plain text: key, summary, type, status, description and up to 100 comments,
converted from Atlassian Document Format. An issue's revision follows its
`updated` time, which new comments also change. `confluence-v1.json` and
`jira-v1.json` bind listings and searches to the selected container; page reads
name a page the adapter checks against its space.

The Box and Dropbox ports read the files directly inside each selected folder,
like Drive's: plain text, Markdown and HTML up to 192 KiB, with other files
observed as unsupported. A file's revision follows the provider's content hash
(Box's SHA-1, Dropbox's `content_hash`), so renames and moves keep it. Box
downloads follow one redirect to `dl.boxcloud.com` without the credential.
Dropbox addresses folders by ID; `dropbox-v1.json` builds each listing body
from the selected folder with a body template, and the continuation takes only
the cursor Dropbox returned. The authority is the Box user ID or the Dropbox
account ID.

The GitHub port reads every issue and pull request in a selected `owner/name`
repository, oldest first, four a page, as Markdown with its labels, body and up
to 20 comments. An issue's revision follows its `updated_at` time, which new
comments also change. The authority is the user's numeric ID. Issues deleted
during a scan can shift later ones to pages already read, so the next scan
restores any it missed. The shared file helpers in `files.rs` also serve the
OneDrive and SharePoint ports.

Every catalog also carries `setup.*` operations with `caller: "control"`. They
are what a connection's picker sends before any run exists, such as listing
Drive folders or Slack channels: names and IDs only, never content, and they
bind no selected container. The broker admits them against the connection's
current grant instead of a run lease, and they share the catalog's quotas with
the runner operations. Teams operations now share one quota rule.

## Control runtime

`nebula-connector-control` verifies Slack, Notion, Google Drive, Gmail Pub/Sub
and Microsoft Graph callbacks. Go supplies encrypted-at-rest verification material
for one subscription; responses bind the tenant, connection, attempt and revisions.
The runtime has no database, keyring or provider network access. It uses a separate
workload identity and accepts only broker traffic at the deployment boundary.

Setup discovery uses `POST /v1/providers/{provider}/discovery`, contract version 1.
A request without a provider response returns an approved `setup.*` operation;
a request with its response returns normalized resources and a parent/query-bound
cursor. The broker executes every HTTP request and retains credentials, quotas
and journaling. Provider bodies and normalized results each have a 2 MiB budget;
the input RPC allows another 32 KiB for context. Slack search continues for at
most five pages or until 200 matches; other pickers return one provider page.
An empty filtered page retains its cursor. This setup API is separate from the
runner's `discover`, which describes already-selected resources.

Set `CONNECTOR_CONTROL_CREDENTIALS_FILE` to a private JSON file containing
`{"tokens":["<32–512 character workload token>"]}`. Two tokens support rotation.
`CONNECTOR_CONTROL_LISTEN` defaults to `127.0.0.1:8092`; terminate TLS at workload
ingress. The endpoint is `POST /v1/providers/{provider}/webhook/verify`.
Each provider has eight admission permits and two verification threads. RPCs have
a two-second deadline, 1 MiB input and 512 KiB callback bounds. Up to 128 HTTP/1
connections each serve one RPC and expire after five seconds; shutdown drains for
three seconds. Timed-out verification retains its permit until the worker finishes.

Shared callback fixtures check signatures, installation identity, challenge gating
and cross-subscription rejection in Rust, plus typed Go decoding. HTTP component
tests cover authentication, independent provider capacity, slow headers and drain;
providers are fixtures, not live integrations. OAuth, subscription setup, JWKS
retrieval remain in progress; Go owns durable callback admission. Rich Graph payloads
are rejected; Notion's initial unsigned setup must provision its verification token
before signed events can be accepted.

Python conformance tests validate wire shapes and schema bounds. Rust tests exercise
actual adapters and transport against local HTTP stubs; they do not qualify live
providers or complete ingestion. Nebula consumers must run the shared fixtures
through their own typed decoders and persistence paths.

## Releases and consumers

Push a `v<workspace version>` tag from `main` to run both architecture checks and
builds. The release workflow publishes a source bundle, Linux runner/control binaries,
SHA-256 checksums and GitHub build attestations. Releases are immutable and marked
prerelease while the subsystem is a foundation preview. Existing versions are never
overwritten; publish a new version for a fix. Crates are consumed from the pinned
source release; this workflow does not publish to crates.io.

Consumers pin the public commit and artifact SHA-256, verify the release workflow's
attestation, and run their own compatibility tests before adopting an update.
Nebula keeps deployment credentials, provider-policy approval and activation in its
private service; an artifact, runner image or connector manifest cannot expand
managed provider access.

```sh
gh release download v0.2.0 --repo zeroset-inc/nebula-connectors \
  --pattern nebula-connectors-source.tar.gz
gh attestation verify nebula-connectors-source.tar.gz \
  --repo zeroset-inc/nebula-connectors \
  --signer-workflow zeroset-inc/nebula-connectors/.github/workflows/release.yml \
  --source-ref refs/tags/v0.2.0 --source-digest <reviewed-commit> \
  --deny-self-hosted-runners
```

## Contributing

Implement `Connector`, declare capabilities and configuration, and add fixtures and
adapter tests. Keep provider-specific behavior in the provider crate; shared wire
rules belong in the SDK. Run the checks above before opening a pull request.
Dependency permissions are enforced by `check.py`; changes need maintainer review.
Version the protocol, provider configuration and cursor formats independently.
Never commit provider credentials or customer data; tests use synthetic fixtures.

## Wire rules

- A connection grant supplies tenant and destination; batch/event payloads
  cannot select them.
- `upsert` carries explicit permission state. `unavailable` means unimplemented;
  `unknown` means fetching or interpreting a policy failed. An empty `known`
  grant set allows nobody when mirroring is active.
- File events reference finalized immutable uploads, with byte length and
  SHA-256. Cloud-native exports also identify their export revision.
- Provider source URLs are provenance, never engine download instructions.
- Searchable metadata has at most 64 keys and only string, number or boolean
  values. Keys require 1–64 UTF-8 bytes, strings at most 1,024 UTF-8 bytes, and
  the serialized object at most 32 KiB. Schema character limits are necessary
  bounds; the Go decoder also enforces these byte limits.
- Withdrawals require provider evidence. The service infers snapshot absence
  only after a complete scan of the matching scope revision.
- JSON rejects duplicate keys, invalid Unicode, NUL characters and the reserved object keys
  `$serde_json::private::Number` and `$serde_json::private::RawValue` at every depth.
  `fixtures/integrity-v1.json` exercises these runtime rules in both typed decoders.
- Checkpoints cover preceding accepted events. A schema-valid checkpoint does
  not by itself authorize advancement or deletion.

See [Go connector foundations](../services/connectors/README.md) for decoder
limits, dependency enforcement and test commands. Generated Rust types do not
replace runtime validation.

File readers require the service's authenticated run `source-eligibility`
endpoint. They check the admitted representation before requesting provider
content and narrow each broker response to the returned byte bound. The decision
reserves no engine capacity; actual source bytes still undergo final validation.
Service compatibility follows this contract, rather than the runner's release age.
