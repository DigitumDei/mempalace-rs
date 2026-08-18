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

### MCP tool surface (43 tools)

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
- `mempalace_lineage_set`
- `mempalace_self_observation_propose`
- `mempalace_self_observation_review`
- `mempalace_identity_packet`
- `mempalace_migration_record`

The five self-continuity tools are local-only. `mempalace_wake_up` compiles the MCP-bound or
palace-default lineage into an identity packet; model-facing calls cannot select or override it.
See [Self-Continuity Across Models](Self-Continuity.md).

### Federation

Added after the initial v1 freeze; now part of the shipped surface.

- `mempalace-server` — Axum REST server exposing a palace, started via
  `mempalace-cli serve`. Bearer-token auth; `GET /v1/health` is public.
- `mempalace-remote` — HTTP client (`RemoteApi` trait + `RemoteClient`).
- `mempalace-federation` — shared wire DTOs.
- REST surface under `/v1`: `info`, `drawers` (search, check_duplicate, add, list,
  get, delete), `kg` (query, facts, facts/invalidate, timeline, stats), `taxonomy`,
  `wings`, `rooms`, `changes`, and `ingest/batch` (bulk mined-chunk ingest).
- Client routing (`federation` config section): per-wing and KG `local` / `remote`
  / `combined` modes with `write` target `local` / `remote` / `both`;
  local-first dual-write with best-effort remote replication in `both` mode;
  federated mining and `mine --branch` branch-delta mining.
- MCP read fan-out: combined search/taxonomy/status, plus `remote_changes` in
  `mempalace_wake_up` and remote merge in `mempalace_get_changes_since`.

See [Federation](Federation.md) for the full guide.

## Explicitly Deferred Or Out Of Scope

- CLI `split` is deferred. It remains visible in help and fails with an explicit deferral
  message pointing at a Phase 9 decision record that is not published in this repository.
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
