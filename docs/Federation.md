# Federation Guide

<div align="center">
  <img src="assets/mempalace-rs-banner.png" alt="Federated MemPalace — a central palace linked to remote palaces" width="100%">
</div>

Federation lets several MemPalace clients share one or more **remote palaces** over
an HTTP REST API. An agent talking to its local MCP server sees a single seamless
palace: reads for selected wings are transparently merged across local and remote,
and writes are routed per the wing's rule. (The `mempalace-cli` is federation-aware
for mining/writes; its `search`/`status`/`wake-up` read the local palace only —
see [Part 5](#part-5--federated-reads-wake-up-and-changes).)

This guide covers the whole feature end-to-end: running a server, configuring a
client, the routing model, federated and branch-aware mining, and how to exercise
it all locally for dev testing.

- For the exact config field reference, see [Config Schema](Config-Schema.md).
- For the locator storage model that federated mining relies on, see
  [Mined Storage](Mined-Storage.md).
- For every CLI flag, see [CLI Surface](CLI-Surface.md).

## Concepts

- **Remote** — a named MemPalace server reachable over HTTP, defined in
  `federation.remotes`. Each remote has a `name`, `url`, optional bearer token,
  and timeout.
- **Route** — per wing (and for the knowledge graph), one of three modes:
  - `local` — served only from the local palace (the default).
  - `remote` — served only from the named remote.
  - `combined` — local and remote are merged on read; `write` selects which side
    new writes go to.
- **Write target** — only meaningful in `combined` mode:
  - `local` — writes go to the local palace only (default).
  - `remote` — writes go to the remote palace only.
  - `both` — **local-first dual-write**: the local write must complete
    successfully before a best-effort remote replication is attempted. Remote
    failure does not roll back the local write or change the success result;
    the outcome of the remote leg is reported as a `replication` field on the
    response.
- **Wing name is the join key.** The same wing name on both sides is treated as
  one combined wing. There is no separate handshake to "link" wings — naming them
  identically is the link.
- **Rank-merge, not score-merge.** Combined search interleaves results by rank
  across origins (round-robin), because similarity scores are not comparable
  across embedding profiles. Every result is annotated with its `origin`
  (`local` or `remote:<name>`).
- **Reads degrade, writes do not (except `both`).** A remote that is unreachable
  during a read is reported as a warning and skipped — the local side still
  returns. A write to a down remote (`write: remote`) is an explicit error with
  **no silent local fallback**. The `write: both` target is the exception: the
  local write must succeed first, and a remote failure is reported as a
  `replication` field on the response without aborting the operation or
  rolling back the local write.
- **Diary is always local.** `wing_agents`, the `diary` room, and `diary:`-prefixed
  sources are hard-pinned to local storage. Any config that tries to route them
  remote is warned about and ignored, and the server rejects diary-shaped writes
  with HTTP 422.

## Part 1 — Running a server (the hub)

The server is the same `mempalace-cli` binary, started with `serve`. It exposes
the local palace at `<palace_path>` over HTTP.

### 1.1 Create a token file

Authentication is bearer-token based. The token file is a JSON array of entries:

```json
[
  { "token": "alice-secret-token", "name": "alice", "enabled": true },
  { "token": "bob-secret-token",   "name": "bob",   "enabled": false },
  { "token": "ci-secret-token",    "name": "ci",     "enabled": true,
    "scopes": [ { "wings": ["wing_myproject"], "operations": ["read", "coordination_read"] } ] }
]
```

- `token` — the bearer secret a client must present.
- `name` — the identity recorded as `added_by` on writes from that token.
- `enabled` — `false` treats the entry as if it did not exist (instant revoke).
- `scopes` — optional; restricts what the token may do. Absent (like `alice` and `bob` above)
  means unrestricted access, exactly as before this field existed. See
  [1.5 Authorization scopes](#15-authorization-scopes) for the full shape and rules.

Tokens are hashed in memory; the raw secret is not retained after load. The file
is hot-reloaded — editing it (e.g. flipping `enabled`, or narrowing `scopes`) takes
effect on the next request without restarting the server. The default path is
`~/.mempalace/server_tokens.json`.

### 1.2 Configure the server section (optional but recommended)

In `~/.mempalace/config.json`:

```jsonc
{
  "server": {
    "bind": "127.0.0.1:8765",
    "token_file": "~/.mempalace/server_tokens.json",
    "checkouts": {
      "wing_myproject": "/srv/repos/myproject",
      "wing_teamdocs":  "/srv/repos/teamdocs"
    }
  }
}
```

`server.checkouts` maps a wing name to a local checkout path on the **server**.
It is how the hub resolves locator-backed mined drawers (see
[Federated mining](#part-3--federated-mining) and [Mined Storage](Mined-Storage.md)):

- **Mapped** — the server reads snippet text from that checkout at search time, so
  results are fresh and non-stale.
- **Unmapped** — the server stores locator rows with an empty root; every result
  for that wing resolves as a *stale placeholder* until you add the mapping, and
  the bulk-ingest response carries a warning. This is safe (no wrong text), just
  degraded.

See [Config Schema → Server Config](Config-Schema.md#server-config) for the full
field reference.

### 1.3 Start the server

```bash
mempalace-cli serve
# or override config:
mempalace-cli serve --bind 0.0.0.0:8765 --token-file /etc/mempalace/tokens.json
```

On start it prints the palace path, bind address, and token file, then logs
`Listening on http://<addr>`. It shuts down gracefully on Ctrl-C.

> **The server speaks plain HTTP.** Bearer tokens cross the wire unencrypted.
> Run it only on a trusted network or behind a TLS-terminating reverse proxy
> (nginx, Caddy, etc.). The server prints this warning on every start.

### 1.4 REST surface

All routes are under `/v1`. `GET /v1/health` is unauthenticated; everything else
requires `Authorization: Bearer <token>`.

| Method & path | Purpose |
|---|---|
| `GET /v1/health` | Liveness probe (no auth) |
| `GET /v1/info` | Server version, `federation_api_version`, embedding profile, capabilities, and maintenance configuration/state |
| `POST /v1/drawers/search` | Semantic search (server embeds the query text) |
| `POST /v1/drawers/check_duplicate` | Near-duplicate check |
| `POST /v1/drawers` | Add a drawer |
| `GET /v1/drawers` | List drawers (paginated) |
| `GET /v1/drawers/{id}` | Get one drawer |
| `DELETE /v1/drawers/{id}` | Delete a drawer |
| `POST /v1/kg/query` | Knowledge-graph query |
| `POST /v1/kg/facts` | Add a KG fact |
| `POST /v1/kg/facts/invalidate` | Invalidate a KG fact |
| `GET /v1/kg/timeline` | KG timeline |
| `GET /v1/kg/stats` | KG statistics |
| `GET /v1/taxonomy` | Wing/room taxonomy |
| `GET /v1/wings` | List wings |
| `GET /v1/rooms` | List rooms |
| `GET /v1/changes` | Change-event feed (cursor-paginated) |
| `POST /v1/ingest/batch` | Bulk mined-chunk ingest (16 MiB body limit) |

`GET /v1/info` advertises a `capabilities` list; the `"ingest"` capability is what
a client checks before attempting federated mining. The wire DTOs live in the
`mempalace-federation` crate and are shared verbatim by server and client.

### 1.5 Authorization scopes

A token with no `scopes` field (§1.1) may do anything any route allows. A scoped
token is restricted to an `(operation, wing)` combination granted by at least one
of its scope entries. `operations` is closed: `read`, `write`, `delete`, `ingest`,
`coordination_read`, `coordination_write`, `coordination_claim`. The three
`coordination_*` operations have no routes yet — they exist so the token file
format is stable ahead of the coordination REST routes.

Every route requires an operation; most also involve a wing. Routes fall into
four groups, and each group is authorized differently:

- **Wing is in the request — enforce `(operation, wing)` directly.**
  `POST /v1/drawers/search`, `GET /v1/drawers` (wing optional in both — when
  given it is enforced outright; when absent, see the next bullet), and
  `POST /v1/drawers` and `POST /v1/ingest/batch` (wing required). A mismatched
  wing is a plain **403**.
- **The wing needs a lookup first.** `GET /v1/drawers/{id}` and
  `DELETE /v1/drawers/{id}` resolve the drawer, then authorize. A caller without
  access gets a **404**, not a 403 — the same masking the diary guard already
  applies to `GET /v1/drawers/{id}` (see `route_drawers_get` in
  `crates/mempalace-server/src/lib.rs`), so the response never becomes an
  existence oracle for wings the caller cannot see.
- **Aggregate routes filter instead of rejecting.** `GET /v1/taxonomy`,
  `GET /v1/wings`, `GET /v1/rooms`, `GET /v1/changes`, and
  `POST /v1/drawers/check_duplicate` require `read`, then filter their response
  down to the wings the token can see — a token scoped to one wing gets that
  wing's slice, not a 403. A wing-absent `POST /v1/drawers/search` does the
  same, filtering ranked candidates after an over-fetch (it has no
  continuation promise, so a short page is an acceptable trade-off there — see
  `route_drawers_search` in `crates/mempalace-server/src/lib.rs`). A
  wing-absent `GET /v1/drawers` cannot use that approach: its `limit`/
  `next_cursor` shape implies a caller can page through everything it can see,
  and the store has no cursor-based pagination, so filtering visibility out of
  an already-`limit`-bounded page could permanently strand authorized rows
  below the page with no way to reach them. `route_drawers_list` instead
  pushes the visible-wing set into the storage query itself
  (`DrawerFilter::wings`, an `IN` match) so storage never returns an
  invisible-wing row in the first place — visibility is enforced by the query,
  not by filtering its output.
  `check_duplicate` belongs here despite having no wing in its own request:
  its response carries `wing`/`room` per match and an `is_duplicate` boolean,
  either of which would otherwise let a token learn about content in a wing it
  cannot read; `is_duplicate` is computed *after* filtering, not before, so
  the boolean itself cannot leak that either. `POST /v1/drawers` applies the
  identical filter to its own near-duplicate check, before deciding whether
  to return **409 duplicate**: candidate matches outside the caller's visible
  wings (and diary matches) are discarded first, because the 409 status and
  its `matches` body would otherwise let a scoped writer learn about content
  in a wing it cannot read even though it can only write to a wing it *is*
  scoped to. This has a real, deliberate cost: a scoped writer can now create
  a drawer that duplicates content already present in a wing it cannot see,
  because that duplicate is invisible to the check. The alternative —
  reporting it — would disclose that wing's content to a caller not
  authorized to read it, which is worse. For an unrestricted token every wing
  is visible, so this changes nothing. `GET /v1/changes` matters most —
  it is the federated change feed. Not every event carries a determinable
  wing (KG facts, identity updates, lineage records and self-observations
  never do; some `drawer_deleted` events written before this scoping model
  existed, or via a remote-fallback delete with no local record of the
  drawer, don't either). The feed fails **closed** on those: an event whose
  wing can't be determined is hidden from a scoped token unless its type is
  one of the seven with no wing concept at all (matching the Group D list
  below); an unrestricted token still sees everything.
- **No wing concept — operation only.** `POST /v1/kg/query`, `GET /v1/kg/timeline`,
  `GET /v1/kg/stats`, `POST /v1/kg/facts`, and `POST /v1/kg/facts/invalidate`
  check only the operation. KG facts are entity-scoped, not wing-scoped — this is
  the same rule `resolve_kg_route` in `mempalace-config` already applies by
  skipping the wing lookup for KG routing. `GET /v1/info` requires any
  authenticated token and no specific operation; `GET /v1/health` stays
  unauthenticated.

The diary guard is unaffected by any of this: it is a content rule (wing
`wing_agents`, room `diary`, or a `diary:`-prefixed source), not an identity
rule, and it applies to every token regardless of scope.

`wings` in a scope entry accepts the literal `"*"` for every wing. Other
entries expand at load time along two independent dimensions, and only one
of them is aliased: **the prefix is aliased, case is significant.**

- **Prefix.** REST and MCP request paths disagree on whether a wing carries
  the `wing_` prefix: REST handlers build the wing they authorize against
  with `WingId::new`, which validates but does not transform, while MCP
  paths use `WingId::normalized`, which adds the prefix when absent. So
  `myproject` and `wing_myproject` genuinely name the same wing by
  convention, and an entry that is a valid `WingId` (passes `WingId::new`)
  but lacks the prefix expands to **two** aliases: the entry as written, and
  the entry with `wing_` prepended — case untouched in both.
- **Case.** `wing_MyProject` and `wing_myproject` are **not** the same wing.
  `WingId::new`'s validation accepts uppercase ASCII and does not transform
  it, so a palace can legitimately hold both as two distinct wings storing
  different data. Folding case into a single alias set — as an earlier
  version of this fix did — would let a scope written for one silently
  authorize the other: a privilege escalation inferred from a spelling
  convention, not a convenience. So an entry that is already a valid
  `WingId`, prefixed or not, keeps its case exactly as written in every
  alias it produces. There is no case-insensitive alias, ever.
- Only when the raw entry is **not** a valid `WingId` at all (e.g. embedded
  whitespace) does it fall back to `WingId::normalized`, which sanitizes and
  lowercases, as its single alias — acceptable there because sanitizing
  malformed input is the whole point of that path and there is no verbatim
  form worth preserving.

Worked examples:

| Raw entry           | Aliases                                    | Why |
|----------------------|---------------------------------------------|-----|
| `wing_MyProject`     | `wing_MyProject`                             | Already prefixed; case is not aliased, so no lowercased sibling. |
| `MyProject`          | `MyProject`, `wing_MyProject`                | Prefix dimension aliases; case preserved in both. |
| `project_alpha`      | `project_alpha`, `wing_project_alpha`        | Same as above, already-lowercase case is just not visible. |
| `My Project` (invalid `WingId`) | `wing_my_project` (via `WingId::normalized`) | No verbatim form exists to preserve, so sanitization applies. |

A request is authorized if it matches any alias a raw entry produces.

## Part 2 — Configuring a client

Clients (the CLI and the MCP server) read `federation` from
`~/.mempalace/config.json`:

```jsonc
{
  "federation": {
    "remotes": [
      {
        "name": "work",
        "url": "https://palace.intra.example",
        "token_env": "MEMPALACE_WORK_TOKEN",
        "timeout_ms": 5000
      }
    ],
    "default_mode": "local",
    "wings": {
      "wing_teamdocs": { "mode": "remote",   "remote": "work" },
      "wing_bigrepo":  { "mode": "combined", "remote": "work", "write": "local" },
      "wing_shared":   { "mode": "combined", "remote": "work", "write": "both" }
    },
    "kg": { "mode": "combined", "remote": "work", "write": "remote" }
  }
}
```

- Prefer `token_env` over an inline `token` so the secret stays out of the config
  file. If the named variable is absent at startup the loader **warns** and
  continues (falling back to inline `token`, or unauthenticated if neither is set)
  — local-only operation never breaks because of a missing remote token.
- `url` must be `http://` or `https://`; any other scheme fails config load.
- A wing whose name matches `server.checkouts` on the hub gets fresh locator
  resolution; otherwise its remote results surface as stale.

### Route resolution precedence

First match wins:

1. Explicit per-wing rule in `federation.wings`
2. The `routing` block in the resolved project declaration (central registry
   first, with repository-local `mempalace.yaml` as the compatibility override)
3. `federation.default_mode`
4. `local` (hard default when no federation config exists)

Then the diary hard-override is applied unconditionally (always local).

The knowledge graph has its own rule (`federation.kg`) because KG facts are
entity-scoped, not wing-scoped — this is what enables the "main facts remote,
branch facts local" pattern.

### Per-project routing

A repo can declare its own route without editing the global config, via the
central project registry or the optional repository-local `mempalace.yaml`:

```yaml
wing: wing_myproject
routing:
  mode: combined
  remote: work
  write: local
```

This sits at precedence step 2 — a global `federation.wings` rule for the same
wing still overrides it.

### `write: both` — local-first dual-write semantics

When `write: both` is configured, every federatable write operation follows a
local-first protocol:

1. **Local write must complete first.** The local storage commit finishes
   before any remote attempt begins.
2. **Best-effort remote replication** is then attempted against the configured
   remote. Transport errors, duplicate rejections, and server errors are all
   caught and reported — they never roll back or abort the local write.
3. **Partial-success reporting.** When the route is `write: both`, the response
   carries a `replication` field typed as
   [`ReplicationStatus`](Config-Schema.md#replicationstatus):
   - `{"status": "replicated", "remote": "<name>"}` — remote succeeded.
   - `{"status": "converged", "remote": "<name>"}` — remote already had
     the exact content (content-hash match); state is converged.
   - `{"status": "failed", "remote": "<name>", "reason": "..."}` — remote
     failed; the local write is unaffected.
   Non-`both` routes and diary-local writes omit the `replication` field
   entirely.
4. **Idempotency of the local write.** The local path uses content-hash
   deduplication for drawer writes and triple-identity checks for KG facts, so
   retrying the whole MCP tool call is safe for the local side. The **remote
   leg** is not universally idempotent: the drawer pre-check uses similarity
   detection (not exact match), and replaying a full MCP `add_drawer` produces
   a new local drawer ID — there is no end-to-end operation ID. Replaying a
   failed remote replication may succeed or encounter a duplicate; in either
   case the local side is not double-written.
5. **No retry is built in.** The replication attempt fires once. Operators
   monitoring `{"status": "failed", ...}` should fix the connectivity issue
   and re-apply the originating write at the MCP tool level. Because there is
   no cross-side operation ID, retry safety depends on operation-specific
   handling — duplicate pre-checks help but are not a full guarantee.
6. **Diary-local-only override still applies.** Even with `write: both`, diary
   targets (`wing_agents`, `diary` room, `diary:`-prefixed sources) are always
   local-only — routing resolves to local before `write: both` is detected, so
   no replication leg is attempted and the response carries no `replication`
   field. No config can federate diary content.

This applies to all federated write paths:
- **Drawer writes** (`mempalace_add_drawer` via MCP) — local commit, then
  pre-check duplicate on remote before writing.
- **KG fact adds** (`mempalace_kg_add`) — local KG commit, then remote.
- **KG fact invalidations** (`mempalace_kg_invalidate`) — local invalidation,
  then remote.
- **Mining** (`mempalace-cli mine` with `write: both`) — local mine completes
  fully (embedding, storage, summary), then a best-effort remote push is
  attempted. The remote result is appended to the mine output; a remote failure
  is reported without rolling back the local mine.

> **DeleteDrawer is excluded from write routing.** `mempalace_delete_drawer` does
> not use `write` target resolution. It always deletes by drawer ID in the local
> palace first. If the drawer is not found locally, it falls back by attempting
> deletion on ALL configured remotes (in deterministic name order), regardless of
> wing routing. This is because dual-written drawers have independent IDs on each
> side with no durable cross-palace ID mapping — routing cannot select the
> "correct" remote by wing. The response reports `applied_to: "local"` or
> `"remote:<name>"` and never carries a `replication` field.

## Part 3 — Federated mining

Mining a project whose wing routes to a remote (`mode: remote`, or `mode: combined`
with `write: remote`) pushes the work to the hub instead of writing locally.

### How it works

1. The CLI runs the full local pipeline — discovery, chunking, byte/line offset
   computation, room detection — but **skips embedding and storage**.
2. It calls `GET /v1/info` and requires the `"ingest"` capability. An older server
   without the endpoint returns 404, surfaced as a clear "upgrade the remote"
   error.
3. Prepared files are sent to `POST /v1/ingest/batch` in batches capped at **64
   files or ~4 MiB of chunk text**, whichever comes first.
4. The **server** embeds each chunk with its own model and commits locator-backed
   drawers, filling `resolve_root` from `server.checkouts[wing]`.
5. Per-file results (`ingested` / `skipped_unchanged` / `failed`) and any warnings
   are aggregated into the mine summary.

```bash
# wing routes to a remote → this pushes to the hub
mempalace-cli mine /path/to/project

# preview what would be sent, no network calls
mempalace-cli mine /path/to/project --dry-run
```

### Machine-independent identity

The hub keys each file by `projects:{wing}:{blake3(repo_id)}:{relative_path}`,
where `repo_id` is the normalized `origin` remote URL (e.g.
`git@github.com:Acme/Repo.git` → `github.com/Acme/Repo`), or `wing:<name>` when no
remote is configured. Because the key is derived from repo identity rather than a
local checkout path, **two clients mining the same repo converge on identical
source keys and drawer ids** — no disjoint histories, and re-pushes dedupe cleanly.

Federated batches are always **canonical**: the hub stamps `view_name: None` on
every row it ingests, and the batch DTOs carry no view metadata. Branch views are
a purely local concept — see [Part 4](#part-4--branch-aware-mining).

### Failure behavior

- Remote unreachable (and mode is `remote` or `combined` with `write: remote`)
  → explicit error, no local fallback.
- Remote unreachable during `write: both` replication → local mine succeeds;
   the mine output appends `Remote replication: failed — <reason>` as a text
   line (not JSON) without rolling back the local mine.
- A bad single file → reported `failed` in the 200 response body; the rest of the
  batch still commits.
- Diary-shaped wing/room → rejected with HTTP 422.

## Part 4 — Branch-aware mining

A branch-delta mine ingests only the files that differ from the default branch,
keeping the local palace in sync with ongoing branch work without re-ingesting the
repo. On a non-canonical checkout this is now the **automatic** behaviour — the
flag only forces it:

```bash
mempalace-cli mine /path/to/project            # auto: canonical on main, branch delta elsewhere
mempalace-cli mine /path/to/project --branch   # force a branch delta
mempalace-cli mine /path/to/project --full     # force a full canonical mine
```

- **Delta** = files changed vs the merge-base with the default branch
  (`origin/HEAD` → `main` → `master`, first that resolves) **plus** untracked
  files. Uncommitted edits are included by design.
- Subdirectory project roots are re-relativized; files outside the project root
  are dropped.
- Branch rows use the `projects-branch` source-key namespace, with the view name as
  its own key segment, so they never collide with a canonical mine of the same wing
  or with another branch view.
- Every run reconciles: drawers for files that have left the delta (reverted,
  merged, rebased away) are removed and reported as `Sources removed: N`. Files
  *deleted* on the branch become tombstones instead, so they keep shadowing the
  canonical snapshot.
- An **auto-detected** branch mine requires an existing canonical snapshot for the
  project; without one it fails and tells you to mine the canonical checkout first
  or pass `--full`. Explicit `--branch` / `--view` bypasses that guard.

> **Federated wings must bypass the guard explicitly.** The check queries the **local**
> palace only. On a wing routed `remote` (or `combined` with `write: remote`) the canonical
> snapshot lives on the hub, so the local lookup finds nothing and an auto-detected branch
> mine fails — even though the canonical data exists. The error's `--full` suggestion does
> not help here either: `--full` is a canonical mine, so it routes straight back to the
> remote and never creates the local snapshot the guard wants. Pass `--branch` or
> `--view <name>` explicitly instead; that marks the mine as deliberate and skips the guard.
- **Branch views are always local**, even for a `remote`/`combined` wing. That is
  the point: they are the local side of a combined wing.

See [Mined Storage → Repository views](Mined-Storage.md#repository-views) for the
detection rules, source-key layout, tombstones, and search-time overlay composition.

### Searching a view

`view` is a first-class search parameter in three places — the MCP `mempalace_search`
tool, the federation wire (`SearchRequest.view`, forwarded to remotes), and the CLI
(`mempalace-cli search --view`). All three take the same values:

- omitted or `"canonical"` — canonical snapshots only
- `"<branch>"` — that branch composed over the canonical snapshot
- `"full"` — every stored repository view, searched independently

In the MCP and REST responses each result carries its own `view` field, absent for canonical
rows. **The CLI does not print it** — `render_search_results` shows wing, room, source,
score, and content only. That matters most for `search --view full`, where rows from
different views can share a source path and the terminal output gives you no way to tell
them apart; use `mempalace_search` or the REST endpoint when you need to attribute a result
to a view.

> **Only `mempalace_search` composes a view across a combined wing.** The CLI's
> `search --view` opens the local `StorageEngine` and never performs federation routing
> (see [Part 5](#part-5--federated-reads-wake-up-and-changes)). On the combined-wing setup
> below — canonical on the hub, branch delta local — `mempalace-cli search --view <branch>`
> returns only the local branch rows, because the canonical rows it would overlay live on
> the remote and the CLI never fetches them. Use the MCP tool for that query.

### The combined-wing team workflow

This is the intended end state of federation + locator storage + branch mining:

1. The team/CI mines the repository's **main** branch into the **remote** shared
   palace (a normal remote-routed `mine`). Done once for the whole team.
2. Each developer mines their **branch delta** into their **local** palace
   (`mine --branch`).
3. The wing is configured `combined`. Search merges both sides; `content_hash`
   deduplication means chunks identical between remote-main and local-branch are
   not double-counted. Pass `view: "<branch>"` to compose the local branch delta
   over the canonical index; without it, search returns canonical rows only.
4. With `write: both`, any additional writes (e.g. authored drawers or
   knowledge-graph facts) land in the local palace and are best-effort
   replicated to the shared remote, keeping both sides in sync without blocking
   the local workflow.

The result: every agent searches one wing and transparently sees shared main
knowledge overlaid with the local branch's in-progress changes, and writes
propagate to the shared palace when the remote is reachable.

## Part 5 — Federated reads, wake-up, and changes

> **Federated reads are an MCP-server capability.** The fan-out and merge below
> happen inside `mempalace-mcp` (the tools your agent calls). The `mempalace-cli`
> `search`, `status`, and `wake-up` commands always operate on the **local**
> palace only — the CLI is federation-aware for **mining (writes)**, not for
> reads. To exercise federated reads, point an MCP client at `mempalace-mcp` with
> the federation config, or call the hub's REST endpoints directly.

Through the MCP server, federated wings fan out across reads:

- **Search / taxonomy / wings / rooms / status** — combined wings merge local and
  remote; results and wings are annotated with origin/availability; a down remote
  becomes a warning, not a failure.
- **`mempalace_wake_up`** — when federation is active, the response gains
  `remote_changes`: a per-remote map of the last 24 h of change events (each event
  carries `origin: "remote:<name>"`), with unreachable remotes shown as
  `{ "unreachable": true, "error": "..." }` and a `next_cursor` per remote.
- **`mempalace_get_changes_since`** — merges local and remote change feeds,
  annotates origin, and accepts per-remote `cursors` for continuation.

> **Clock-skew caveat:** persist and pass back the per-origin cursors rather than
> comparing timestamps across machines. Cross-machine `occurred_at` ordering is
> best-effort display only.

## Part 6 — Dev testing locally

You can exercise the whole feature on one machine with two palace directories. The
hub does the embedding, so set `MEMPALACE_STUB_EMBEDDINGS` (deterministic vectors,
no model download) on the **hub** process — the client never embeds during a
remote mine.

```bash
# 1. Hub palace + token file
mkdir -p /tmp/hub
echo '[{"token":"dev-token","name":"dev","enabled":true}]' > /tmp/hub/tokens.json

# 2. Start the hub against the hub palace (stub embeddings for a fast offline run)
MEMPALACE_STUB_EMBEDDINGS=1 mempalace-cli --palace /tmp/hub/palace serve \
  --bind 127.0.0.1:8765 --token-file /tmp/hub/tokens.json &

# 3. Point a client config at the hub (client uses a different, default palace)
#    ~/.mempalace/config.json:
#    { "federation": {
#        "remotes": [{ "name": "hub", "url": "http://127.0.0.1:8765", "token": "dev-token" }],
#        "wings": { "wing_demo": { "mode": "remote", "remote": "hub" } } } }

# 4. Mine a project whose mempalace.yaml declares wing: wing_demo → pushes to the hub.
#    The client does NOT embed here; the hub does.
mempalace-cli mine /path/to/demo-project

# 5. Verify the hub received it. The CLI's own `search` only reads the LOCAL
#    palace, so query the hub directly over REST instead:
curl -s -X POST http://127.0.0.1:8765/v1/drawers/search \
  -H "Authorization: Bearer dev-token" -H "Content-Type: application/json" \
  -d '{"query":"something from the project","wing":"wing_demo","limit":5}'
```

To exercise federated **reads** (combined search, wake-up fan-out), point an MCP
client at `mempalace-mcp` running with the same client config from step 3 — the
fan-out lives in the MCP server, not the CLI.

For non-stale snippet resolution on the hub, add the project path to
`server.checkouts.wing_demo` in the hub's config and restart `serve`.

Notes:
- `MEMPALACE_STUB_EMBEDDINGS` is honored by the `mempalace-mcp` binary and the CLI
  `serve` command. The CLI `mine`/`search` commands always use the real embedding
  provider — but a *remote-routed* `mine` does no client-side embedding at all, so
  the stub setting only matters on the hub. For a fully stubbed read path, run the
  MCP server with the env var set.
- The stub maps keyword-less text to a single vector, so give demo files distinct
  keyword clusters if you want them to rank apart.

## Troubleshooting

### `remote '<name>' is unreachable ... writes do not fall back to local`
The hub is down or the URL/port is wrong. Federated writes with `write: remote`
intentionally do not fall back — fix connectivity or switch the wing to a
different target. For `write: both`, the local write still succeeds; check the
`replication` field on the MCP tool response for the failure detail.

### `remote '<name>' does not support the ingest capability`
The hub is an older build without `POST /v1/ingest/batch`. Upgrade the server.

### Search results from a remote wing are all stale placeholders
The hub has no `server.checkouts` entry for that wing. Add the mapping and restart
`serve`; existing rows resolve fresh on the next search (no re-mine needed).

### Config load warns about a missing token env var
`token_env` names a variable that is not set. Export it, or the client proceeds
unauthenticated (fine against a server with an unauthenticated entry, otherwise
401s on protected routes).

### A remote-routed write returns 422
The target is diary-shaped (`wing_agents` / `diary` room / `diary:` source). Diary
is local-only by design; no config can federate it.

### A request returns 403 with code `forbidden`
The token authenticated fine but its `scopes` do not grant the operation or wing
the request needs — distinct from a 401, which means the token itself was
missing, unrecognised, or disabled. Check the token's `scopes` entry in the
server's token file: either the `operations` list is missing the one the route
needs, or none of its `wings` cover the target wing (`"*"` covers every wing).
See [1.5 Authorization scopes](#15-authorization-scopes). `GET /v1/drawers/{id}`
and `DELETE /v1/drawers/{id}` mask this as 404 instead — see the same section.

### `version != 1` on the server
The server only accepts config schema version `1`. See
[Config Schema](Config-Schema.md).
