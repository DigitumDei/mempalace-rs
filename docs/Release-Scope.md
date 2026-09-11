# Rust v1 Release Scope

This document defines the first Rust release surface for `agentpalace`.

## In Scope

### Executable and runtime

- `agentpalace` (`agentpalace.exe` on Windows): CLI, HTTP, and stdio MCP
- Statically linked ONNX Runtime 1.23.2 and upstream license notices

### CLI commands frozen for v1

- `init`
- `mine` — canonical, branch-delta (`--branch` / `--view` / automatic detection), and
  remote-routed mining; see Federation
- `project` — `register`, `show`, `list`, `remove`, `export` against the central registry
- `prune` — scoped deletion of mined project data from the local palace
- `search` — including `--view` scoping over repository views
- `status`
- `wake-up`
- `setup` — register the MCP server with detected AI coding tools
- `maintain` — one-shot maintenance pass (compact, prune, optimize)
- `serve` (HTTP MCP plus federation REST, or `--stdio`; see Federation below)

Full flag reference: [CLI Surface](CLI-Surface.md).

### Storage shape frozen for v1

- Palace root contains `storage.sqlite3` for operational state.
- Palace root contains `lancedb/` for drawer vectors and retrieval data.
- `storage.sqlite3` also contains provider-neutral agent lineages, reviewed self-observations,
  review history, and model/harness migration records used to compile identity packets.
- Mined project files are stored as **locator rows** (byte/line ranges resolved lazily from
  the checkout), tagged with **repository-view metadata** so canonical snapshots and branch
  deltas coexist and compose at search time. See [Mined Storage](Mined-Storage.md).

### Maintenance

- Enabled by default; runs in the background of the HTTP hub (`serve`) and on demand via
  `agentpalace maintain`. Three tiers — vector-index optimization, fragment compaction,
  version retention — coordinated by a cross-process SQLite advisory lease. See
  [Operator guide](Operator-Standard.md#maintenance).

### Runtime profiles frozen for v1

- `balanced`
- `low_cpu`

### MCP tool surface (69 tools)

- `agentpalace_wake_up`
- `agentpalace_status`
- `agentpalace_list_wings`
- `agentpalace_coordination_wings`
- `agentpalace_list_rooms`
- `agentpalace_get_taxonomy`
- `agentpalace_get_aaak_spec`
- `agentpalace_kg_query`
- `agentpalace_kg_add`
- `agentpalace_kg_invalidate`
- `agentpalace_kg_timeline`
- `agentpalace_kg_stats`
- `agentpalace_traverse`
- `agentpalace_find_tunnels`
- `agentpalace_graph_stats`
- `agentpalace_search`
- `agentpalace_check_duplicate`
- `agentpalace_add_drawer`
- `agentpalace_delete_drawer`
- `agentpalace_diary_write`
- `agentpalace_diary_read`
- `agentpalace_get_changes_since`
- `agentpalace_identity_read`
- `agentpalace_identity_update`
- `agentpalace_task_create`
- `agentpalace_task_list`
- `agentpalace_task_get`
- `agentpalace_task_claim`
- `agentpalace_task_renew`
- `agentpalace_task_transition`
- `agentpalace_message_send`
- `agentpalace_message_get`
- `agentpalace_message_acknowledge`
- `agentpalace_inbox_read`
- `agentpalace_artifact_put`
- `agentpalace_artifact_get`
- `agentpalace_result_put`
- `agentpalace_result_get`
- `agentpalace_coordination_event_get`
- `agentpalace_coordination_events`
- `agentpalace_skill_propose`
- `agentpalace_skill_get`
- `agentpalace_skill_versions`
- `agentpalace_skill_list`
- `agentpalace_skill_record_outcome`
- `agentpalace_skill_promote`
- `agentpalace_skill_retire`
- `agentpalace_skill_reviews`
- `agentpalace_delegation_span_start`
- `agentpalace_delegation_span_get`
- `agentpalace_delegation_span_close`
- `agentpalace_delegation_spans_for_task`
- `agentpalace_delegation_checkpoint_append`
- `agentpalace_delegation_checkpoint_get`
- `agentpalace_delegation_trace`
- `agentpalace_lineage_set`
- `agentpalace_self_observation_propose`
- `agentpalace_self_observation_review`
- `agentpalace_identity_packet`
- `agentpalace_migration_record`
- `agentpalace_a2a_agent_card`
- `agentpalace_a2a_task_import`
- `agentpalace_a2a_task_export`
- `agentpalace_a2a_message_import`
- `agentpalace_a2a_artifact_import`
- `agentpalace_mcp_tasks_get`
- `agentpalace_mcp_tasks_update`
- `agentpalace_mcp_tasks_cancel`
- `agentpalace_mcp_tasks_import`

The nine protocol-adapter tools (`agentpalace_a2a_*`, `agentpalace_mcp_tasks_*`) translate between
AgentPalace coordination records and the A2A and `io.modelcontextprotocol/tasks` wire models
(issue #102 Stages 9-10). They are all local-only: the import tools translate *and* persist,
which is a two-write sequence (the record, then the raw wire envelope stored as a
`protocol_envelope` artifact) with no remote transaction available to make it atomic, so no
remote path is offered rather than one that can half-apply. Neither adapter has an HTTP surface
of its own; this tool surface is the only entry point.

The five self-continuity tools are local-only. `agentpalace_wake_up` compiles the MCP-bound or
palace-default lineage into an identity packet; model-facing calls cannot select or override it.
See [Self-Continuity Across Models](Self-Continuity.md).

The eight skill-registry tools and the seven delegation-telemetry tools are local-only and
are not federated. The coordination task, message, artifact, result, and event tools are
federation-aware as of issue #102 Stage 4, opt-in per wing via `federation.coordination`:
`agentpalace_task_create` routes
by the task's wing; every other ID-keyed tool (get/claim/renew/transition, message send/get/ack,
artifact/result put/get) tries local storage first and falls back to each configured remote in
name order on a local miss; and the two aggregate feeds (`agentpalace_inbox_read`,
`agentpalace_coordination_events`) always read local and, when coordination federation is
configured at all, fan out concurrently to the remotes a `federation.coordination` rule names
(plus the default remote when `default_mode` is not `local`), each with its own cursor. A read
filtered to `wing_agents` never fans out, on either feed. `agentpalace-server` exposes the same records over `/v1/coordination/*` under
the same scoped-token authorization as every other route. See
[Native Coordination](Coordination.md), [Federation](Federation.md#part-7--federated-coordination),
[Skill Registry](Skill-Registry.md), and [Delegation Telemetry](Delegation-Telemetry.md).

`agentpalace_coordination_wings` is a local discovery read. It lists the union of wings observed
in local coordination tasks and events, wings named by configured coordination routes, and the
effective default wing (even when the local palace has no coordination rows). Each entry reports
the effective write destination (`local` or `remote:<name>`), an `is_default` marker, and
provenance markers (`configured`, `in_use`, and `built_in`) identifying configured routes or a
configured default, local records, and the built-in fallback. The list describes locally known
scope; legacy `wing_unscoped` rows are excluded as a reserved pre-wing marker; it does not perform
an exhaustive scan of remote palaces.

### Federation

Added after the initial v1 freeze; now part of the shipped surface.

- `agentpalace-server` — Axum REST server exposing a palace, started via
  `agentpalace serve`. Bearer-token auth; `GET /v1/health` is public.
- `agentpalace-remote` — HTTP client (`RemoteApi` trait + `RemoteClient`).
- `agentpalace-federation` — shared wire DTOs.
- REST surface under `/v1`: `info`, `drawers` (search, check_duplicate, add, list,
  get, delete), `kg` (query, facts, facts/invalidate, timeline, stats), `taxonomy`,
  `wings`, `rooms`, `changes`, `ingest/batch` (bulk mined-chunk ingest), and — added in
  issue #102 Stage 3 — `coordination` (tasks: create/list/get/claim/renew/transition; messages:
  send/get/ack, inbox; artifacts: put/get; results: put/get; events: the cursor-paginated
  audit feed). See [Federation §1.4](Federation.md#14-rest-surface).
- Client routing (`federation` config section): per-wing and KG `local` / `remote`
  / `combined` modes with `write` target `local` / `remote` / `both`;
  durable local-first dual-write with **asynchronous, queued replication** in
  `both` mode (issue #127): the tool commits locally and returns
  `replication.status: "queued"` with a stable `operation_id`, a background
  worker delivers idempotently, and outcomes surface via `agentpalace_status`
  (`replication.backlog` / `replication.recent_terminal_failures`) and
  `replication.phase_metrics`; federated mining and `mine --branch` branch-delta
  mining.
- Structured federation outcomes (issue #127 review hardening): direct `write:
  remote` mutations whose outcome is unconfirmed return a structured
  `outcome: "unknown_outcome"` result (remote + stable `operation_id` + safe-retry
  guidance) rather than a generic internal error, and a keyed `write: both` delete
  retried after the local delete replays the original queued/terminal outbox state
  by the caller `operation_id`. Combined reads report partial failures both as
  legacy string `warnings` and as a machine-actionable `degradations` array
  (`code`, `remote`, `kind`, `error`, `classification`).
- MCP read fan-out: combined search/taxonomy/status, plus `remote_changes` in
  `agentpalace_wake_up`, remote merge in `agentpalace_get_changes_since`, and (issue #102 Stage 4)
  `remote_messages`/`remote_events` in `agentpalace_inbox_read`/`agentpalace_coordination_events`.
- Routing discovery (issue #125): when federation has remotes configured, `agentpalace_status`,
  `agentpalace_list_wings`, `agentpalace_list_rooms`, and `agentpalace_get_taxonomy` each include a
  `wing_availability` map (drawer routing *mode*, `federation.wings`, values `"local"` /
  `"remote:<name>"` / `"combined"`) and a sibling `coordination_availability` map (the effective
  task *write target*, `federation.coordination`, values `"local"` / `"remote:<name>"` only —
  `"combined"` cannot occur here because a coordination route can never resolve to `write:
  both`), both keyed by wing name. The two maps can disagree for the same wing — drawer and
  coordination routing are independent tables — and `coordination_availability`'s key set
  includes wings named only in `federation.coordination` (no drawers, no `federation.wings`
  entry), which `wing_availability` never surfaces. See
  [Federation → Discovering coordination routing](Federation.md#discovering-coordination-routing).

See [Federation](Federation.md) for the full guide.

## Breaking Changes

Changes that alter an already-shipped surface, newest first. A consumer written against the
previous shape must be updated; nothing here is additive.

### v0.1.26 — coordination task-write responses (issue #102 Stage 4)

`agentpalace_task_claim`, `agentpalace_task_renew` and `agentpalace_task_transition` changed shape
twice in one release, in the same direction: a revision conflict is now **data**, not an error,
and the task is **nested** rather than spread across the top level.

| | Before | After |
|---|---|---|
| Success | bare task object — `{"task_id": ..., "revision": ...}` | `{"success": true, "task": {...}}` |
| Conflict | JSON-RPC error | `{"success": false, "conflict": {"expected_revision": N, "actual_revision": N \| null, "message": "..."}}` |

Two reasons this was worth breaking. A stale revision is an ordinary, expected outcome of
compare-and-swap under contention — modelling it as a transport error forced every caller to
parse an error string to distinguish "retry with the current revision" from "this genuinely
failed". And the federated path had drifted: a remote claim returned the task flattened with
`success` beside its fields, so a client written against the local shape lost the task entirely
whenever the same call fell back to a remote. Both paths now emit one envelope, pinned by a
test that drives the same assertion over each.

This matches `agentpalace_skill_promote` and `agentpalace_delegation_span_close`, which have used
the `{"success", ...}` envelope since Phase 2.

**Migrating:** read `response.task` instead of the response body, and branch on
`response.success` instead of catching a JSON-RPC error. `actual_revision` is `null` when the
record does not exist at any revision, as opposed to existing at a different one.

Note that the two Phase 2 tools above still do not describe their own response envelope in their
tool `description`, which is a pre-existing documentation gap rather than a change here.

## Explicitly Deferred Or Out Of Scope

- CLI `split` is deferred. It remains visible in help and fails with an explicit deferral
  message pointing at the [Phase 9 deferral record](rust-phase-plans/Phase09-Deferred-Commands.md).
- CLI `compress` is deferred, on the same terms as `split`.
- Federated **branch views**. `POST /v1/ingest/batch` is canonical-only; the batch DTOs carry
  no view metadata, so branch deltas stay in the client's local palace.
- AAAK reverse parsing is deferred for Rust v1.
- Automatic Wikipedia or other networked entity enrichment is out of scope.
- Python-era state inspection and import are not part of the default Rust release scope unless Phase 10 is explicitly reopened.
- OS-native installers or package-manager distributions are not defined here; the current release artifact is the Cargo-built binary set.

## Known Limitations

- Final benchmark and low-CPU signoff must be performed on the reference environment, not assumed from a generic VM.
- `init` performs embedding startup validation and reports the resulting status, but model acquisition is still an operator-managed step.
- Low-CPU mode clamps ingest, search, and wake-up limits; it is a product mode, not a claim that every host will meet target budgets automatically.

## Release Rule

If a behavior is not documented in this directory and is not covered by the frozen command or tool surface above, it should not be treated as a Rust v1 release promise.
