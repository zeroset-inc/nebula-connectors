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

## Control runtime

`nebula-connector-control` verifies Slack, Notion, Google Drive, Gmail Pub/Sub
and Microsoft Graph callbacks. Go supplies encrypted-at-rest verification material
for one subscription; responses bind the tenant, connection, attempt and revisions.
The runtime has no database, keyring or provider network access. It uses a separate
workload identity and accepts only broker traffic at the deployment boundary.

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
gh release download v0.1.0 --repo zeroset-inc/nebula-connectors \
  --pattern nebula-connectors-source.tar.gz
gh attestation verify nebula-connectors-source.tar.gz \
  --repo zeroset-inc/nebula-connectors \
  --signer-workflow zeroset-inc/nebula-connectors/.github/workflows/release.yml \
  --source-ref refs/tags/v0.1.0 --source-digest <reviewed-commit> \
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
