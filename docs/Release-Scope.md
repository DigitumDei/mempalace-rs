# Rust v1 Release Scope

This document defines the first Rust release surface for `mempalace-rs`.

## In Scope

### CLI binaries

- `mempalace-cli`
- `mempalace-mcp`

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
- `serve` (federation HTTP server; see Federation below)

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
  `mempalace-cli maintain`. Three tiers — vector-index optimization, fragment compaction,
  version retention — coordinated by a cross-process SQLite advisory lease. See
  [Operator guide](Operator-Standard.md#maintenance).

### Runtime profiles frozen for v1

- `balanced`
- `low_cpu`

### MCP tool surface (58 tools)

- `mempalace_wake_up`
- `mempalace_status`
- `mempalace_list_wings`
- `mempalace_list_rooms`
- `mempalace_get_taxonomy`
- `mempalace_get_aaak_spec`
- `mempalace_kg_query`
- `mempalace_kg_add`
- `mempalace_kg_invalidate`
- `mempalace_kg_timeline`
- `mempalace_kg_stats`
- `mempalace_traverse`
- `mempalace_find_tunnels`
- `mempalace_graph_stats`
- `mempalace_search`
- `mempalace_check_duplicate`
- `mempalace_add_drawer`
- `mempalace_delete_drawer`
- `mempalace_diary_write`
- `mempalace_diary_read`
- `mempalace_get_changes_since`
- `mempalace_identity_read`
- `mempalace_identity_update`
- `mempalace_task_create`
- `mempalace_task_get`
- `mempalace_task_claim`
- `mempalace_task_renew`
- `mempalace_task_transition`
- `mempalace_message_send`
- `mempalace_message_get`
- `mempalace_message_acknowledge`
- `mempalace_inbox_read`
- `mempalace_artifact_put`
- `mempalace_artifact_get`
- `mempalace_result_put`
- `mempalace_result_get`
- `mempalace_coordination_event_get`
- `mempalace_coordination_events`
- `mempalace_skill_propose`
- `mempalace_skill_get`
- `mempalace_skill_versions`
- `mempalace_skill_list`
- `mempalace_skill_record_outcome`
- `mempalace_skill_promote`
- `mempalace_skill_retire`
- `mempalace_skill_reviews`
- `mempalace_delegation_span_start`
- `mempalace_delegation_span_get`
- `mempalace_delegation_span_close`
- `mempalace_delegation_spans_for_task`
- `mempalace_delegation_checkpoint_append`
- `mempalace_delegation_checkpoint_get`
- `mempalace_delegation_trace`
- `mempalace_lineage_set`
- `mempalace_self_observation_propose`
- `mempalace_self_observation_review`
- `mempalace_identity_packet`
- `mempalace_migration_record`

The five self-continuity tools are local-only. `mempalace_wake_up` compiles the MCP-bound or
palace-default lineage into an identity packet; model-facing calls cannot select or override it.
See [Self-Continuity Across Models](Self-Continuity.md).

The eight skill-registry tools and the seven delegation-telemetry tools are local-only and
are not federated. The fifteen coordination tools, by contrast, are federation-aware as of
issue #102 Stage 4, opt-in per wing via `federation.coordination`: `mempalace_task_create` routes
by the task's wing; every other ID-keyed tool (get/claim/renew/transition, message send/get/ack,
artifact/result put/get) tries local storage first and falls back to each configured remote in
name order on a local miss; and the two aggregate feeds (`mempalace_inbox_read`,
`mempalace_coordination_events`) always read local and, when coordination federation is
configured at all, fan out concurrently to the remotes a `federation.coordination` rule names
(plus the default remote when `default_mode` is not `local`), each with its own cursor. A read
filtered to `wing_agents` never fans out, on either feed. `mempalace-server` exposes the same records over `/v1/coordination/*` under
the same scoped-token authorization as every other route. See
[Native Coordination](Coordination.md), [Federation](Federation.md#part-7--federated-coordination),
[Skill Registry](Skill-Registry.md), and [Delegation Telemetry](Delegation-Telemetry.md).

### Federation

Added after the initial v1 freeze; now part of the shipped surface.

- `mempalace-server` — Axum REST server exposing a palace, started via
  `mempalace-cli serve`. Bearer-token auth; `GET /v1/health` is public.
- `mempalace-remote` — HTTP client (`RemoteApi` trait + `RemoteClient`).
- `mempalace-federation` — shared wire DTOs.
- REST surface under `/v1`: `info`, `drawers` (search, check_duplicate, add, list,
  get, delete), `kg` (query, facts, facts/invalidate, timeline, stats), `taxonomy`,
  `wings`, `rooms`, `changes`, `ingest/batch` (bulk mined-chunk ingest), and — added in
  issue #102 Stage 3 — `coordination` (tasks: create/get/claim/renew/transition; messages:
  send/get/ack, inbox; artifacts: put/get; results: put/get; events: the cursor-paginated
  audit feed). See [Federation §1.4](Federation.md#14-rest-surface).
- Client routing (`federation` config section): per-wing and KG `local` / `remote`
  / `combined` modes with `write` target `local` / `remote` / `both`;
  local-first dual-write with best-effort remote replication in `both` mode;
  federated mining and `mine --branch` branch-delta mining.
- MCP read fan-out: combined search/taxonomy/status, plus `remote_changes` in
  `mempalace_wake_up`, remote merge in `mempalace_get_changes_since`, and (issue #102 Stage 4)
  `remote_messages`/`remote_events` in `mempalace_inbox_read`/`mempalace_coordination_events`.
- Routing discovery (issue #125): when federation has remotes configured, `mempalace_status`,
  `mempalace_list_wings`, `mempalace_list_rooms`, and `mempalace_get_taxonomy` each include a
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

`mempalace_task_claim`, `mempalace_task_renew` and `mempalace_task_transition` changed shape
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

This matches `mempalace_skill_promote` and `mempalace_delegation_span_close`, which have used
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
