# Standard Operator Guide

Native coordination state is stored in `storage.sqlite3` and follows the same backup and restore boundary as other operational SQLite data. Preserve this file to retain task revisions, leases, inbox acknowledgements, results, artifacts, idempotency records, and event cursors. See [Native local coordination](Coordination.md#recovery-and-maintenance).

This guide covers the normal Rust deployment path for `mempalace-cli` and `mempalace-mcp`.

## Prerequisites

- Rust toolchain compatible with workspace `rust-version = 1.88`
- Writable home directory for `~/.mempalace`
- Writable cache directory for embedding assets

## Build

From the `mempalace-rs` directory:

```bash
cargo build --release -p mempalace-cli -p mempalace-mcp
```

Expected binaries:

- `target/release/mempalace-cli`
- `target/release/mempalace-mcp`

## First-Time Bootstrap

1. Initialize project-local room config.

```bash
target/release/mempalace-cli init /path/to/project
```

2. Confirm the reported startup validation status.

Expected statuses:

- `ready`
- `missing_assets`
- `partial_download`
- `corrupted_cache`

3. Ingest data.

```bash
target/release/mempalace-cli mine /path/to/project
```

4. Validate retrieval.

```bash
target/release/mempalace-cli search "auth migration"
target/release/mempalace-cli status
target/release/mempalace-cli wake-up
```

## Paths And State

Default state roots:

- global config: `~/.mempalace/config.json`
- palace root: `~/.mempalace/palace`
- default embeddings cache root: platform cache dir under `mempalace/embeddings`

Set `MEMPALACE_CONFIG_DIR` to move the `~/.mempalace` base directory itself
(`config.json`, `projects.json`, `people_map.json`, and the default
`server_tokens.json` location all move with it) — see
[Config-Schema.md](Config-Schema.md#other-environment-variables). This is
orthogonal to `MEMPALACE_PALACE_PATH`/`--palace`, which move only the palace
root and take precedence over it for that one path.

Presence checks used by the CLI:

- `storage.sqlite3`
- `lancedb/`

If neither exists, the CLI treats the palace as missing and prints bootstrap guidance.

## Model Acquisition And Warm Cache

Operational rule:

- Do not treat `init` as proof that assets are already present.
- Treat the startup validation status as the source of truth.
- By default both `mempalace-cli` and `mempalace-mcp` stay offline and will not download embedding assets.
- Set `MEMPALACE_EMBED_ALLOW_DOWNLOADS` to an explicit truthy value (`1`, `true`, or `yes`) on first run when you want either binary to bootstrap missing model assets into the local cache.

Recommended sequence:

1. Run `init`.
2. If validation is not `ready`, either:
   set `MEMPALACE_EMBED_ALLOW_DOWNLOADS=1` and re-run the command to let the binary fetch missing assets, or
   pre-stage/repair the embedding cache out of band before relying on offline operation.
3. Run a small `mine` or `search` flow to warm the chosen profile on the target host.
4. Re-run `search` once to confirm warm-path behavior before calling the host production-ready.

Example first-run bootstrap:

```bash
MEMPALACE_EMBED_ALLOW_DOWNLOADS=1 target/release/mempalace-cli mine /path/to/project
```

## MCP Deployment

The MCP binary is the Rust server entrypoint:

```bash
target/release/mempalace-mcp
```

The server exposes the frozen v1 tool set listed in [Release Scope](Release-Scope.md).

Set `MEMPALACE_LINEAGE_ID` in an MCP host's server environment to bind wake-up and identity
packets to one lineage. The binding is validated by `mempalace-mcp` and cannot be overridden by
model-facing tool arguments. If its target does not exist, the response uses the palace default and
includes instructions for creating the requested lineage with `mempalace_lineage_set`. Leave it
unset to use the palace default. See [Self-continuity](Self-Continuity.md#binding-a-lineage-to-an-mcp-client).

If the MCP host needs to bootstrap a cold cache on first start, launch it with:

```bash
MEMPALACE_EMBED_ALLOW_DOWNLOADS=1 target/release/mempalace-mcp
```

## Federation Server Deployment

To share a palace with other clients, run the federation HTTP server:

```bash
target/release/mempalace-cli serve --bind 127.0.0.1:8765 \
  --token-file ~/.mempalace/server_tokens.json
```

Operational notes:

- Create the token file first — a JSON array of objects, each with `token`,
  `name`, and `enabled` keys. It is hot-reloaded, so revoking a token (set
  `enabled: false`) takes effect on the next request without a restart.
- Scope tokens with an optional `scopes` array — restrict a token to specific
  wings and operations (`read`, `write`, `delete`, `ingest`, plus three
  `coordination_*` operations reserved for a future release) instead of
  granting it everything. Omitting `scopes` keeps a token unrestricted, which
  is why token files written before this field existed still work unchanged;
  an explicit `"scopes": []` is the opposite — a deliberate lockout with no
  access at all. Like `enabled`, scope edits are picked up on the next request
  via the same hot reload — no restart needed to tighten or loosen a token's
  access. See [Federation → 1.5 Authorization scopes](Federation.md#15-authorization-scopes)
  for the full model, including which routes reject a wing mismatch outright
  (403) versus mask it as a 404 or filter it out of an aggregate response.
- The server speaks **plain HTTP**. On any untrusted network, run it behind a
  TLS-terminating reverse proxy; never expose raw bearer tokens over the wire.
- `GET /v1/health` is unauthenticated and suitable as a liveness probe; all other
  routes require `Authorization: Bearer <token>`, and — for a scoped token — the
  right scope for that route.
- To resolve mined locator snippets server-side, map wings to local checkout paths
  via `server.checkouts` in `config.json`.
- Cold cache bootstrap uses the same `MEMPALACE_EMBED_ALLOW_DOWNLOADS` rule as the
  other binaries; `MEMPALACE_STUB_EMBEDDINGS` runs the server with deterministic
  stub vectors for offline testing.

Full setup, client configuration, and the team mining workflow are in the
[Federation guide](Federation.md).

## Maintenance

The maintenance subsystem keeps the palace storage healthy by compacting
fragments, pruning old version data, and optimising vector indices. It is
**enabled by default** and the long-lived HTTP hub (`mempalace-cli serve`)
schedules it automatically by default. Set `background_enabled: false` to
use manual-only maintenance; the one-shot CLI command (`mempalace-cli maintain`)
remains available while `enabled` is `true`.

### Maintenance Tiers

Each run executes up to three tiers in order:

1. **Vector Index Optimization** — rebuilds LanceDB vector indices for
   faster approximate-nearest-neighbour search.
2. **Fragment Compaction** — merges small LanceDB fragments to reduce
   storage overhead and improve scan performance.  Triggered when the
   number of small fragments exceeds `small_fragment_threshold` (default:
   `10`).
3. **Version Retention** — removes version data rows older than
   `version_retention_hours` (default: `24`).

### Configuration Defaults

| Field | Default | Description |
|---|---|---|
| `enabled` | `true` | Master switch for all maintenance, including `mempalace-cli maintain`. |
| `background_enabled` | `true` | Whether the HTTP hub schedules maintenance automatically. |
| `idle_secs` | `300` | Minimum wall-clock seconds since the last write before a run starts. |
| `version_retention_hours` | `24` | Maximum age in hours for retained version rows. |
| `tail_threshold_rows` | `1024` | Row count that triggers incremental vector-index optimization. |
| `small_fragment_threshold` | `10` | Fragment count that triggers small-fragment compaction. |

### Environment Overrides

All six fields can be overridden at process start via environment
variables, which take precedence over `config.json`:

- `MEMPALACE_MAINTENANCE_ENABLED` — true values: `1`, `true`, `TRUE`, `yes`,
  `YES`; false values: `0`, `false`, `FALSE`, `no`, `NO`. Other values are rejected.
- `MEMPALACE_MAINTENANCE_BACKGROUND_ENABLED` — same boolean values; set it to
  `false` for manual-only maintenance while retaining `mempalace-cli maintain`.
- `MEMPALACE_MAINTENANCE_IDLE_SECS` — positive integer; zero is rejected.
- `MEMPALACE_MAINTENANCE_VERSION_RETENTION_HOURS` — positive integer;
  zero is rejected.
- `MEMPALACE_MAINTENANCE_TAIL_THRESHOLD_ROWS` — positive integer; zero
  is rejected.
- `MEMPALACE_MAINTENANCE_SMALL_FRAGMENT_THRESHOLD` — positive integer;
  zero is rejected.

### Idle-Only Hub Scheduling

When `background_enabled` is `true`, the HTTP hub (`mempalace-cli serve`) runs
maintenance in a background tokio task. The scheduling rules are:

- **Startup eligibility check**: on hub startup, one maintenance
  eligibility check runs immediately.  The storage engine initialises its
  activity timestamp at open time, so the first actual run occurs only
  after the configured `idle_secs` interval has elapsed without write
  activity (subject to the lease gate — see below).
- **Loop**: after each run, the task sleeps for `idle_secs` plus a
  randomised jitter of up to 10% of `idle_secs` to desynchronise
  concurrent hubs.
- **Idle reset**: every incoming HTTP request signals activity via the
  `activity_middleware`.  The middleware calls `signal_activity()` on the
  storage engine and notifies the background task, which cancels any
  pending sleep and restarts the idle timer.
- **Not idle**: if the background task wakes from sleep and detects
  recent activity (elapsed time < `idle_secs`), it skips the run and
  goes back to sleep.
- **Write-path safety**: write operations (add, delete, mine, ingest)
  also call `signal_activity()`, so maintenance never runs concurrently
  with active writes from the same process.

The one-shot CLI command (`mempalace-cli maintain`) bypasses the
process-local idle gate entirely (sets `idle_secs` to `0`) so the pass
runs immediately.  It still respects the cross-process lease.

### Cross-Process Lease Semantics

Maintenance is coordinated across concurrent processes via a single-row
SQLite advisory lease stored in the palace's `storage.sqlite3`:

- **Claim**: a process must atomically insert or update the lease row
  with its holder ID and a 5-minute TTL.  If another process holds a
  valid (non-expired) lease, the claim is denied.
- **Renewal**: while a tier is executing, the orchestrator re-asserts
  the lease every 60 seconds, extending the TTL by 5 minutes.  If
  renewal fails (e.g. the database connection is interrupted), the
  `lease_lost` flag is set and subsequent tiers are skipped.
- **Release**: after all tiers complete, the lease is released so the
  next contender can proceed immediately.
- **Expiry**: if the holding process crashes or is killed, the lease
  expires after 5 minutes and any contender can reclaim it.
- **`busy_timeout = 0`**: lease acquisition opens a dedicated SQLite
   connection with a zero busy timeout so the claim never blocks on
   database contention.  A denied lease (another process holds it) is
   returned as a non-error skip, not a retry.  Lease renewal and release
   use the normal storage connection, which may block briefly under
   contention.

This ensures that multiple hubs, CLI invocations, or a mix of both
never run maintenance simultaneously on the same palace.

### Observability

The hub exposes maintenance state through two channels:

**`GET /v1/info` (federation API)**

The `InfoResponse` body includes these maintenance fields:

| Field | Type | Description |
|---|---|---|
| `maintenance_enabled` | `bool` | Whether the subsystem is enabled. |
| `maintenance_background_enabled` | `bool` | Whether the HTTP hub schedules maintenance automatically. `false` indicates manual-only maintenance when `maintenance_enabled` is `true`. |
| `maintenance_idle_secs` | `u64` | Configured idle threshold. |
| `maintenance_last_run` | `serde_json::Value` or `null` | Full JSON-serialized [`MaintenanceRunSummary`] of the last maintenance attempt/run summary. Contains `run_id`, `started_at`, `finished_at`, `duration`, `cpu_duration`, `status`, and `tier_results`. |
| `maintenance_status` | `MaintenanceStatus` | Typed status enum: `disabled`, `idle`, `running`, `skipped { reason }`, `aborted { reason }`, `failed { message }`, `completed { status }`. Replaces the ambiguity of a null `maintenance_last_run`. |

**Structured logs**

The storage engine emits `tracing` events (`info`/`warn`/`error`) at key
points during each run, including structured fields like `tier`,
`tail_threshold`, `wall_ms`, `cpu_ms`, and `idle_secs`.  These are
visible in the hub's stderr output and can be ingested by any
`tracing`-compatible observability pipeline.

### One-Shot CLI for Large Existing Palaces

For a palace that has accumulated significant fragmentation or version
data before the maintenance subsystem was introduced (e.g. upgrading
from an older MemPalace release), the recommended procedure is:

1. **Back up** the palace root (`storage.sqlite3` and `lancedb/`
   together) before running maintenance, in case of unexpected issues.
2. **Run the one-shot CLI command**:
   ```bash
   mempalace-cli maintain --palace /path/to/palace
   ```
3. **Inspect the output** for per-tier outcomes.  Tiers report
   `completed`, `skipped {reason}`, `aborted {reason}`, or `failed`.
   A `success` or `partial` overall status is expected.
4. **If you run multiple CLI processes concurrently** (e.g. in a CI
   matrix), only the first to claim the SQLite lease will execute;
   the others will report `aborted {concurrent_run}`.
5. **After the initial one-shot pass**, the hub's background maintenance
   will handle incremental compaction and pruning automatically during
   idle periods when `background_enabled` is `true`. When it is `false`,
   schedule further `mempalace-cli maintain` runs yourself.

The `maintain` command respects the same `enabled`, `version_retention_hours`,
`tail_threshold_rows`, and `small_fragment_threshold` settings from
`config.json` (or environment overrides).  The only difference from
background hub behaviour is that the idle gate is bypassed, so the pass
starts immediately.

## Reclaiming Space From Mined Data

`mempalace-cli prune` deletes mined project data from the **local** palace by scope. It
previews by default and only deletes with `--yes`:

```bash
# preview everything mined for one project
mempalace-cli prune --project-id github.com/acme/repo

# drop a single stale branch view
mempalace-cli prune --project-id github.com/acme/repo --view old-feature --yes

# drop one subtree of a branch view
mempalace-cli prune --project-id github.com/acme/repo --view old-feature \
  --source-prefix crates/legacy/ --yes
```

Operational notes:

- Prune refuses to run without a narrow scope: pass `--project-id`, or both `--wing` and
  `--kind`. This is deliberate — there is no "prune everything" form.
- Only the two project ingest kinds (`projects`, `projects-branch`) are reachable. Diary,
  narrative, and authored drawers cannot be pruned by this command.
- It never touches a remote palace, even for a wing routed remote.
- Data mined before the stable project-id migration is keyed by checkout path and will not
  match `--project-id`. Re-mining migrates the **canonical** rows; legacy `projects-branch`
  rows are not migrated or cleaned by any mine, so sweep those with `--wing` +
  `--kind projects-branch` after checking the preview.

### `--source-prefix` matches project-root-relative paths

The prefix is matched against the stored source key, whose trailing segment is the file
path **relative to the project root that was mined** — `crates/legacy/foo.rs`, not an
absolute or repo-parent-relative path. Two consequences worth knowing before you reach for
it:

- **Without `--view` it only touches the canonical snapshot.** Branch views are skipped
  (the view name precedes the path in the key), and the skip is reported as a `Note` line.
  Pair it with `--view` to prune inside a branch.
- **It will not clean up linked Git worktrees mined as their own project.** Those files are
  stored relative to the worktree root, so their paths look like `src/lib.rs` — a
  `--source-prefix .claude/worktrees/` scope matches nothing. A prefix like that only
  matches a palace mined *before* discovery began skipping linked worktrees, where the
  parent checkout's mine pulled the worktree in as ordinary subdirectories.

### Pruning a linked worktree

A linked worktree does **not** get its own project identity by default.
`derive_project_id` reads `origin` from the worktree, which is the repository's origin, and
`project_root_relative` resolves against the worktree's own toplevel — so the derived ID is
byte-identical to the main checkout's.

That makes `prune --project-id <repo-id> --yes` the wrong tool: it matches the whole
repository, deleting the canonical snapshot and **every** branch view along with the
worktree's rows. Scope it instead by what actually distinguishes the worktree's data:

```bash
# a worktree mined on its own branch is a branch view — prune that view
mempalace-cli prune --project-id github.com/acme/repo --view worktree-branch --yes
```

If you need worktrees to be independently prunable, give them a distinct identity **at mine
time** with an explicit `--project-id`; only then does a project-scoped prune isolate them.
`--wing` plus `--kind` is broader still and will cross projects sharing that wing.

Always read the `Matched: N sources` preview before adding `--yes`; a scope that matches
nothing prints `Nothing matched this scope.` rather than failing.

Full flag reference: [CLI Surface → `prune`](CLI-Surface.md#prune).

## Storage Recovery

If the palace is damaged or inconsistent:

1. Stop writes to the affected palace root.
2. Inspect whether `storage.sqlite3` and `lancedb/` both exist.
3. If only one store survived, do not assume the state is complete.
4. Restore both from the same backup point when possible.
5. Re-run ingest from source data for any interval that cannot be restored consistently.

Operational guidance:

- Back up `storage.sqlite3` and `lancedb/` together.
- Do not back up only one side of the storage layout and assume point-in-time consistency.

## Durable Replication Recovery

`write: both` federation routes are asynchronous and durable (issue #127): the
MCP process commits the local mutation immediately and persists a replication
intent in `replication_outbox` (inside `storage.sqlite3`), then a background
worker delivers it to the remote with a stable `operation_id`. Because the
outbox lives in the same SQLite file as the palace schema, operator backup and
recovery guidance above applies to it unchanged.

Observe the replication pipeline through `mempalace_status` (and the status
embedded in `mempalace_wake_up`):

- `replication.backlog` — pending/leased/retryable counts, the age of the oldest
  pending operation, the attempt count and last error of the oldest retryable
  operation, plus failed/cancelled totals. A non-zero `retryable_count` or
  `pending_count` means some queued write has not reached the remote yet.
- `replication.recent_terminal_failures` — the newest operations that hit an
  authoritative permanent error (e.g. HTTP 401, or a semantic/content duplicate
  with a different remote `drawer_id`). Each entry carries the `operation_id`,
  remote, mutation kind, attempt count, and `last_error`.
- `replication.phase_metrics` — per-phase latency aggregates
  (`duplicate_search`, `embedding`, `commit`, `outbox_wait`,
  `delivery_attempt`, `remote_acknowledge`) with count/last/total/max/avg
  millisecond statistics. These are process-local and reset on restart; they
  are also emitted as `tracing` events under target `mempalace_metrics`.

Operational rules:

- A remote outage does **not** require re-applying the originating MCP tool
  call. The queued intent retries indefinitely with bounded exponential backoff
  and jitter; fix the remote and watch `replication.backlog` drain.
- A `recent_terminal_failures` entry is the terminal answer: the remote
  authoritatively rejected that mutation (wrong credentials, or a stable-
  identity conflict the receiver would repeat on every retry). Resubmitting the
  tool call unchanged will reproduce the same terminal result. Investigate the
  `last_error` instead.
- On restart, startup reconciliation settles intents whose local mutation never
  committed: committed mutations are activated and delivered, uncommitted
  intents are cancelled. You do not need to intervene for a clean shutdown, but
  a hard power loss mid-write leaves exactly this two-step recovery to the next
  `McpServer` start.

## Troubleshooting

### `No palace found at ...`

Cause:
- No initialized palace exists at the resolved path.

Response:
- Run `init` and `mine`, or point `--palace` to the correct palace root.

### `version != 1` config failure

Cause:
- The runtime only accepts config schema version `1`.

Response:
- Rewrite the config to the frozen v1 schema or remove the file and let `init` recreate it.

### Startup validation is `partial_download` or `corrupted_cache`

Cause:
- Embedding assets are incomplete or invalid.

Response:
- Repair the selected model cache before relying on offline operation.

### Search or wake-up returns fewer results than expected

Cause:
- Wing or room filters may be narrowing the search.
- Low-CPU mode may be clamping result counts.

Response:
- Check the resolved profile and low-CPU settings in `config.json`.
