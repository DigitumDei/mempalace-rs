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
| `POST /v1/coordination/tasks` | Create a task |
| `GET /v1/coordination/tasks/{id}` | Get one task |
| `POST /v1/coordination/tasks/{id}/claim` | Claim a task (or reclaim an expired lease) |
| `POST /v1/coordination/tasks/{id}/renew` | Renew a live lease |
| `POST /v1/coordination/tasks/{id}/transition` | Transition a task's lifecycle state |
| `POST /v1/coordination/messages` | Send an addressed message |
| `GET /v1/coordination/messages/{id}` | Get one message |
| `POST /v1/coordination/messages/{id}/ack` | Acknowledge a message |
| `GET /v1/coordination/inbox` | Read an addressed inbox (cursor-paginated) |
| `POST /v1/coordination/artifacts` | Store an immutable artifact |
| `GET /v1/coordination/artifacts/{id}` | Get one artifact |
| `POST /v1/coordination/results` | Store an immutable task result |
| `GET /v1/coordination/results/{id}` | Get one task result |
| `GET /v1/coordination/events` | Coordination audit-event feed (cursor-paginated) |

`GET /v1/info` advertises a `capabilities` list; the `"ingest"` capability is what
a client checks before attempting federated mining, and the `"coordination"`
capability (added in issue #102 Stage 3) is what a client would check before
calling any `/v1/coordination/*` route — see
[Part 7](#part-7--federated-coordination). The wire DTOs live in the
`mempalace-federation` crate and are shared verbatim by server and client.

### 1.5 Authorization scopes

A token with no `scopes` field (§1.1) may do anything any route allows. A scoped
token is restricted to an `(operation, wing)` combination granted by at least one
of its scope entries. `operations` is closed: `read`, `write`, `delete`, `ingest`,
`coordination_read`, `coordination_write`, `coordination_claim`. The three
`coordination_*` operations gate the `/v1/coordination/*` routes added in issue
#102 Stage 3 — see [Part 7](#part-7--federated-coordination) for how wing
authorization, actor identity, and cursors work on those routes specifically;
the group rules below (A–D) describe the routes that existed before Stage 3.

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

You can exercise the whole feature on one machine with two palaces — a hub and a
client — each with its own `config.json`. Use `MEMPALACE_CONFIG_DIR` to give the
client its own `~/.mempalace`-shaped directory instead of editing your real one;
the hub is pointed at directly with `--palace`/`--token-file` and needs no config
directory of its own. The hub does the embedding, so set
`MEMPALACE_STUB_EMBEDDINGS` (deterministic vectors, no model download) on the
**hub** process — the client never embeds during a remote mine.

```bash
# 1. Hub palace + token file
mkdir -p /tmp/hub
echo '[{"token":"dev-token","name":"dev","enabled":true}]' > /tmp/hub/tokens.json

# 2. Start the hub against the hub palace (stub embeddings for a fast offline run)
MEMPALACE_STUB_EMBEDDINGS=1 mempalace-cli --palace /tmp/hub/palace serve \
  --bind 127.0.0.1:8765 --token-file /tmp/hub/tokens.json &

# 3. Give the client its own config directory instead of touching
#    ~/.mempalace/config.json — MEMPALACE_CONFIG_DIR redirects config.json,
#    projects.json, and (by default) the client's own palace under it.
mkdir -p /tmp/client
cat > /tmp/client/config.json <<'JSON'
{
  "version": 1,
  "federation": {
    "remotes": [{ "name": "hub", "url": "http://127.0.0.1:8765", "token": "dev-token" }],
    "wings": { "wing_demo": { "mode": "remote", "remote": "hub" } }
  }
}
JSON
export MEMPALACE_CONFIG_DIR=/tmp/client

# 4. Mine a project whose mempalace.yaml declares wing: wing_demo → pushes to the hub.
#    The client does NOT embed here; the hub does. MEMPALACE_CONFIG_DIR must be
#    set in this shell (or exported) so the CLI picks up /tmp/client/config.json.
mempalace-cli mine /path/to/demo-project

# 5. Verify the hub received it. The CLI's own `search` only reads the LOCAL
#    palace, so query the hub directly over REST instead:
curl -s -X POST http://127.0.0.1:8765/v1/drawers/search \
  -H "Authorization: Bearer dev-token" -H "Content-Type: application/json" \
  -d '{"query":"something from the project","wing":"wing_demo","limit":5}'
```

To exercise federated **reads** (combined search, wake-up fan-out), point an MCP
client at `mempalace-mcp` launched with `MEMPALACE_CONFIG_DIR=/tmp/client` in its
environment (an MCP client typically sets this in its server-launch config, since
`mempalace-mcp` itself takes no CLI arguments) — the fan-out lives in the MCP
server, not the CLI.

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

## Part 7 — Federated coordination

Issue #102 Stage 3 exposes [native coordination](Coordination.md) — tasks, addressed
messages, immutable artifacts and results, and the audit-event feed — over the same
`/v1/coordination/*` REST surface and scoped-token authorization used by everything
else in this guide.

As of issue #102 Stage 4, the client side is wired up too: `RemoteApi`, `FederationRouter`,
and the coordination MCP tools (`mempalace_task_create` and friends) route to a configured
remote — see [Client-side coordination routing](#client-side-coordination-routing) below.

### Wing is the authorization key

A task's `wing` is what every coordination authorization check is ultimately about,
because messages, artifacts, and results carry no wing column of their own — they
reach it through their mandatory `task_id`, same as locally (see
[Coordination.md](Coordination.md)). Three rules cover all fourteen routes:

- **Task creation reads the wing from the request body.** `POST /v1/coordination/tasks`
  authorizes `(coordination_write, wing)` directly, the same as `POST /v1/drawers`
  does for `write` — a mismatched wing is a plain **403**. `parent_id` and every
  entry in `dependencies` are also resolved and authorized (as `coordination_read`)
  before the task is created: a candidate id in a wing the caller cannot see is
  rejected with the identical **404** an unauthorized-wing task lookup produces
  elsewhere on this surface (see the next bullet), specifically so this route
  cannot be used to probe whether a hidden id in another wing exists.
- **Every other route resolves the wing from the target record first.** `GET`/`claim`/
  `renew`/`transition` on a task look the task up and authorize against its `wing`;
  sending a message, filing an artifact, or storing a result looks up the `task_id`
  named in the request body and authorizes against *its* `wing`; getting one message,
  artifact, or result looks up that record and then its owning task. A caller who
  cannot see the resolved wing gets **404, not 403** — identical in spirit to
  `GET /v1/drawers/{id}` (§1.5) — so the response is never an existence oracle for a
  task in a wing the caller cannot see. A missing record and a wing-invisible record
  return the exact same 404 body; there is no way to tell them apart from outside.
- **`wing_agents` never federates, on any coordination route.** This is the same
  diary hard-override as everywhere else in this guide (§Concepts), applied
  unconditionally regardless of the token's scope. `POST /v1/coordination/tasks`
  with `"wing": "wing_agents"` fails config-independently with **422**
  `diary_not_federated`, same as `POST /v1/drawers`. Every other route — including
  a task that somehow already exists in `wing_agents` from before this rule — masks
  it as a plain **404** on a read, and returns the same 422 on a write (claim,
  renew, transition, message, artifact, or result). The inbox and event feeds never
  surface a `wing_agents` row, whether unfiltered or explicitly filtered to it.
- **The event feed and the inbox are aggregates, not lookups.** `GET /v1/coordination/events`
  and `GET /v1/coordination/inbox` can return rows from many tasks and many wings in
  one page, so — like `GET /v1/changes` and `GET /v1/rooms` — they filter their
  response down to the wings the caller can see rather than rejecting the request.
  An explicit `?wing=` filter that the caller cannot see yields an empty page, not a
  403. Every `coordination_events` row carries its own `wing` column, materialised
  from the owning task at write time and defaulting to `wing_unscoped` for rows that
  predate wings (see [Coordination.md](Coordination.md)) — so, unlike the generic
  `/v1/changes` feed, there is no "wing could not be determined" case to reason
  about; filtering fails closed simply because an unlisted wing is never in the
  visible set.
- **Filtering happens before the pagination cursor is computed, not after.** Wing
  visibility (and the `wing_agents` exclusion above) is enforced inside the storage
  query for both feeds, so an invisible row never reaches the `LIMIT`/cursor
  boundary in the first place. `next_cursor` is therefore always computed over the
  rows this caller can actually see: it cannot be used to learn whether a hidden
  wing has more rows than fit on the current page, or how many rows it has at all.
  An explicit `?wing=` naming an invisible or diary wing that genuinely has rows
  returns the identical `next_cursor: null` a wing with zero rows returns. See
  [Coordination-Phase-3-Design.md](Coordination-Phase-3-Design.md) (deviation 7).
- **An idempotency-key replay re-authorizes the record it actually returns.**
  `POST /v1/coordination/tasks`, `.../messages`, `.../artifacts`, and `.../results`
  are idempotent on `(actor, idempotency_key)`: replaying a key returns the
  originally-created record, on whatever wing it actually lives in — which is not
  necessarily the wing (or task) named in the replay request, e.g. if the caller's
  scope has since narrowed, or if it reuses a key across two different wings. Before
  serialising that returned record, the server re-checks the *returned* wing against
  the caller's current scope. When that check fails, the response is **409**
  `idempotency_key_conflict`, not the 200 the stale pre-write check might suggest,
  and not a 404 — acknowledging that the key already exists discloses nothing
  beyond what the caller already knows about its own identity, but the response
  names neither the wing nor any field of the record itself.

### Actor identity is derived, not claimed

Locally, a caller supplies its own actor string (`created_by`, `sender`, `worker`,
`actor`) and the host runtime is trusted to have asserted it truthfully. Over HTTP
that trust boundary does not exist, so the authenticated token identity is
authoritative: every actor-shaped field in a coordination request body is optional
and is treated as a *claim*, not a fact. The server computes the actual actor as

```
identity                      if the field is absent, or equals the identity
{identity}:{claimed}          if the field is present and differs from the identity
```

— the identical rule `POST /v1/drawers` already applies to `added_by`. A token
authenticated as `ci` that posts `{"created_by": "alice"}` gets a task recorded as
`created_by: "ci:alice"`, never `"alice"` — a remote caller cannot impersonate a
local actor. This applies to `created_by` (tasks, artifacts, results), `sender`
(messages), `worker` (claim/renew), and `actor` (transition, message ack).

A claimed value containing `:` is rejected with **400** rather than folded into
the `identity:claimed` string: `:` is the delimiter that string uses, so a claim
containing it would make the encoding ambiguous with a differently-named token —
e.g. a token named `ci` claiming actor `worker` would otherwise produce
`ci:worker`, identical to the principal of a distinct token whose *configured*
`name` literally is `ci:worker`. The other half of that ambiguity is closed by
rejecting `:` in a token's `name` at token-file load time — see
[Config Schema → `server.token_file`](Config-Schema.md#servertoken_file).
`recipient` on a message is **not** rewritten this way: it addresses a message to
someone, and does not itself assert who the caller is, so it is stored verbatim —
matching local behaviour.

`POST /v1/coordination/messages/{id}/ack`'s `actor` field is a deliberate exception
to the identity-derivation rule above, not an application of it. Storage requires
the final actor to equal the message's `recipient` **exactly** — and `recipient`,
per the paragraph above, is itself stored verbatim, never identity-prefixed. Running
the claimed ack actor through the same `{identity}:{claimed}` rule as every other
actor field therefore made a federated acknowledgement fail whenever the remote
token's identity differed from the recipient's name, since a prefixed string can
never equal an unprefixed one. The server instead uses the claimed actor **as-is**
when it exactly matches the message's `recipient` — proving you know the
(unauthenticated) address a message was already sent to, no stronger a claim than
the sender who chose that address in the first place — and falls back to the
ordinary `{identity}:{claimed}` derivation for any other claim, so a claim naming
some unrelated identity still cannot be recorded bare: it is either exactly the
recipient (allowed) or it gets prefixed and then fails the recipient-equality
check (rejected), never both. Only `actor` on the ack route works this way; `sender`
(message send) and every other actor-shaped field still follow the derivation rule
unconditionally.

### Cursors are opaque strings with no clock in them

`next_cursor` on `GET /v1/coordination/events` and `GET /v1/coordination/inbox` is a
string that encodes only `coordination_events.sequence` / the message's local
sequence — an `AUTOINCREMENT` integer, already monotonic per palace. Pass it back
verbatim; do not parse it, and do not do arithmetic on it. This is deliberately
**not** the `"{rfc3339}|{rowid}"` shape `encode_cursor` uses for `GET /v1/changes`
(§1.4): that format carries a timestamp because `/v1/changes` supports a `since`
parameter and therefore needs time ordering, while the coordination feeds have no
`since` — a bare sequence satisfies "cursors support restart-safe delivery without
relying on synchronized clocks" more directly than reusing a timestamp-bearing
format would. Cursors are per-origin: a cursor from one palace means nothing to
another, so a client combining several remotes keeps one cursor per remote (as
`mempalace_get_changes_since` already does for the generic feed) rather than
comparing or merge-sorting them.

### Lease and expiry clocks belong to the palace that owns the task

`POST /v1/coordination/tasks/{id}/claim` and `.../renew` take a `lease_seconds`
duration, never a timestamp. Expiry — both for that lease and for a task's own
`expires_at` — is evaluated entirely by `CoordinationStore` using the palace's own
clock at the moment of the call. No route accepts or forwards a caller-supplied
timestamp for a lease or expiry decision, so nothing here depends on clocks being
synchronised across machines. This matches the non-goal in
[Coordination-Phase-3-Design.md](Coordination-Phase-3-Design.md): a task is
authoritative in exactly one palace, and that palace's clock is the only one that
gets a vote.

`lease_seconds` must be a positive integer no greater than 100 years' worth of
seconds; a value outside that range fails with **400** before the request reaches
storage. This bound exists because `now + lease_seconds` is ordinary date
arithmetic — a `lease_seconds` large enough to push the result past what a
timestamp can represent is rejected explicitly rather than the request failing in
an unspecified way.

### Revision and lease conflicts

A `claim`/`renew`/`transition` whose `expected_revision` no longer matches the
task's current revision, or that otherwise conflicts with the task's current state
or lease ownership, returns **409** with one of two `code` values in the body:

- `"revision_conflict"` — the expected revision is stale. The body also carries
  `expected_revision` and `actual_revision` (both integers), so the caller can
  decide whether to reload and retry without a second round trip. Retry is the
  caller's decision; the server never retries a conflicting write on its behalf.
- `"coordination_conflict"` — the write is not permitted regardless of revision:
  another worker's lease has not expired yet, the task is in a terminal state, the
  requested state transition is not a valid one, or the caller is not the current
  owner. `expected_revision`/`actual_revision` are both `null` here — reloading
  will not change the outcome.

### A minimal example

```bash
# Create a task in wing_myproject
curl -s -X POST http://127.0.0.1:8765/v1/coordination/tasks \
  -H "Authorization: Bearer dev-token" -H "Content-Type: application/json" \
  -d '{"title":"index the repo","description":"...","wing":"wing_myproject","idempotency_key":"job-1"}'
# → {"task_id":"task_...","state":"pending","revision":0,"created_by":"dev",...}

# Claim it (worker identity comes from the token; expected_revision is CAS)
curl -s -X POST http://127.0.0.1:8765/v1/coordination/tasks/task_.../claim \
  -H "Authorization: Bearer dev-token" -H "Content-Type: application/json" \
  -d '{"expected_revision":0,"lease_seconds":300}'
# → {"state":"running","revision":1,"owner":"dev",...}
```

### Client-side coordination routing

Issue #102 Stage 4 wires the routes above into `RemoteApi`, `FederationRouter`, and the
coordination MCP tools (`mempalace_task_create` and friends) documented in
[Coordination.md](Coordination.md), so an agent talking to its local MCP server can reach a
configured remote's coordination state the same way it already reaches remote drawers and KG
facts.

**The capability gate.** Every coordination method on `RemoteClient` checks the cached
`GET /v1/info` `capabilities` list for `"coordination"` before sending any request. A remote
that does not advertise it (an older, pre-Stage-3 server) fails with
`RemoteError::CapabilityMissing { remote, capability }` — a clear, non-degradable error naming
the remote and the missing capability, not a `404` that could be confused for "the record
doesn't exist." `RemoteApi`'s coordination methods all have default trait bodies returning this
same shape, so a test double or a future `RemoteApi` implementor that has no reason to support
coordination compiles and behaves the same way without implementing any of them.

**A separate routing table, and why.** `resolve_coordination_route(federation, wing)` reads a
dedicated `federation.coordination` table — the same shape as `federation.wings`, but a
different table, not a different lookup into the same one. A task is authoritative in exactly
one palace (see the no-multi-master non-goal in
[Coordination-Phase-3-Design.md](Coordination-Phase-3-Design.md)), so **`write: both` on a
`federation.coordination` entry is a hard config-load error** — sharing `federation.wings`
would mean that error retroactively broke any wing already configured for the documented,
encouraged dual-write-drawers workflow (see [Part 4](#part-4--branch-aware-mining)) the moment
that same wing also carried coordination traffic. The diary hard-override still applies exactly
as it does everywhere else: `wing_agents` coordination is always local, unconditionally,
regardless of any `federation.coordination` entry.

```jsonc
{
  "federation": {
    "coordination": {
      "wing_myproject": { "mode": "remote", "remote": "work" },
      "wing_shared":    { "mode": "combined", "remote": "work", "write": "local" }
      // "write": "both" here is rejected at config load, even though it is legal
      // (and common) on the equivalent federation.wings entry for the same wing.
    }
  }
}
```

**The routed wing is normalised before either the diary check or the table lookup runs.**
`mempalace_task_create` calls `WingId::normalized` on the caller-supplied wing once, up front,
and uses that canonical value for the route decision *and* for the outgoing request (local or
remote) alike — a short or mixed-case spelling (`"agents"`, `"Wing_Agents"`, `"myproject"`) is
routed exactly as its canonical form (`wing_agents`, `wing_myproject`) would be. This matters
because `resolve_coordination_route`'s diary check and its `federation.coordination` map lookup
are both plain string comparisons against the canonical `wing_*` form; routing on a raw,
un-normalised string could otherwise let a short-form `wing_agents` spelling slip past the diary
hard-override, or let a short-form spelling of an operator's explicit `local`-pinned wing miss
the map and fall through to `default_mode`. `resolve_coordination_route` itself also normalises
defensively, so the guard holds even if some future caller forgets to normalise first; when the
given wing cannot be normalised at all, it resolves local rather than falling through to
`default_mode`, since a routing decision that gates data egress must fail closed, not open, on
an input it cannot canonicalize.

**`mempalace_task_create` is the one wing-routed write.** It is also the only coordination
request that carries a wing at all — every other coordination tool (`mempalace_task_get`,
`mempalace_task_claim`, `mempalace_message_send`, and so on) acts on an existing task,
message, artifact, or result ID, and none of them take a `wing` argument, so there is nothing
for `resolve_coordination_route` to resolve against. Those ID-keyed tools instead use a
**local-first, ID-discovery fallback**: local storage is tried first; on a miss, if coordination
federation is configured at all, the router tries each **candidate** remote in name order and
uses whichever one actually owns the record — mirroring `mempalace_delete_drawer`'s existing "no
cross-palace ID mapping" reasoning exactly. The candidate set is every remote named by a
`federation.coordination[wing]` rule (across every wing — there is no wing to key a single
lookup by, so it is the union) plus `default_remote` when `default_mode` is non-`local`; a
remote configured only for drawer or KG federation, never referenced by coordination
configuration at all, is skipped entirely rather than probed and misread as a coordination
failure. This fallback fires independent of any *specific* wing's resolved mode — it is not
gated by a `combined`/`remote` rule for the record's own wing, since that wing is not yet known
when the fallback starts — but it does require coordination federation to be configured at all:
either `federation.coordination` has at least one entry, or `default_mode` is non-`local` (which
is what `resolve_coordination_route` itself falls through to for any wing without an explicit
entry). A palace that federates drawers only, with an empty `federation.coordination` table and
`default_mode: local`, never sends a coordination ID lookup to any remote — a configured remote
alone is not enough.

**Reads and writes agree that a `404` and a missing `coordination` capability both mean "not
this palace, try the next candidate"; they part ways on everything else.** Both a `404`-shaped
rejection and `RemoteError::CapabilityMissing` are a *definitive* answer of absence — "no such
record" and "I don't implement coordination at all", respectively, the latter read live from
the candidate's own `/v1/info`, independent of how `federation.coordination` describes it, so a
candidate remote can still turn out not to run coordination at all. Neither is a sign the
configured remote is broken, so both fallbacks skip past them to the next candidate. For a
**read** (`mempalace_task_get`, `mempalace_message_get`, `mempalace_artifact_get`,
`mempalace_result_get`), a genuinely-degradable `Unreachable` remote is *also* skipped — the
federation-wide "reads degrade" rule, so one down remote never blocks discovery through the
others — but every remaining error from a candidate — wrong credentials or an incompatible API
version — means that *configured* remote is broken in a way that cannot be read as "absent",
and is raised as a tool error instead of being folded into `{"found": false}`; a caller must be
able to tell "this record genuinely does not exist anywhere" apart from "your token is wrong"
or "this remote is on an incompatible protocol version" — cases where the record might still
exist and reporting absence would be a lie. For a **write**
(`mempalace_task_claim`/`_renew`/`_transition`, `mempalace_message_send`,
`mempalace_message_acknowledge`, `mempalace_artifact_put`, `mempalace_result_put`), every error
other than `404`/`CapabilityMissing`, including an unreachable remote, is terminal: unlike a
read, a write cannot afford to guess past a candidate it could not get a definitive answer from,
since guessing wrong could create a second, divergent record for the same task on the wrong
palace. (An earlier revision of this fallback pair put `CapabilityMissing` in the read side's
terminal set instead of its skippable one, contradicting the write side and hard-erroring every
coordination read against a remote that simply predates coordination support; see deviation 21
in [Coordination-Phase-3-Design.md](Coordination-Phase-3-Design.md).) A read that finds the
record on a remote annotates the response with `origin: "remote:<name>"`; a write that lands on
a remote reports `applied_to: "remote:<name>"`. `mempalace_task_claim`/`_renew`/`_transition`'s
successful remote response nests the task under `"task"`, exactly like the local shape
documented below — `{"success": true, "task": {...}, "applied_to": "remote:<name>"}` — so a
caller never has to special-case which palace served the write.

**The ID-discovery read fallback probes candidates sequentially, one at a time, stopping at the
first success — this is deliberate, not an oversight left over from before the fan-outs went
concurrent.** `mempalace_inbox_read`/`mempalace_coordination_events` fan out concurrently because
they are aggregate reads: every candidate's answer is wanted, so nothing is lost by asking them
all at once. `coordination_read_fallback` is a discovery lookup for one record: the moment a
candidate answers, the search stops, so a candidate after the winner is never contacted in the
first place. Making it concurrent would not change what is returned — only what is sent: every
configured coordination candidate would receive the id being looked up on every local miss,
unconditionally, including the remotes that never had the record. Per this document's governing
invariant that memory never leaves the user's control by default, broadcasting a caller's query
id to remotes with no answer to it is a real data-minimisation regression, bought only with
latency on a path that already runs after a local miss — exactly the case local-first ordering
exists to keep off the network. Sequential order is load-bearing here, not incidental.

`mempalace_inbox_read` and `mempalace_coordination_events` are the exception to the exception:
being aggregate, cursor-paginated feeds (like `mempalace_get_changes_since`), they always read
local and fan out concurrently, with a per-remote cursor, to every remote in
`FederationRouter::coordination_candidates()` — **not** every configured remote; a remote wired
up only for drawer or KG federation, never named by any `federation.coordination` rule, is
skipped entirely, the same candidate set the ID-discovery fallbacks above use (see
`coordination_candidate_remotes`). Results are reported under `remote_messages`/`remote_events`
— never routed by a single wing's rule, and never merged into the local page — matching
`changes_fanout`'s `{unreachable, error}` isolation contract for genuine failures (one down
remote never blocks a healthy one). A candidate that answers `CapabilityMissing` — decided live
from its own `/v1/info`, so a remote named by a coordination rule can still turn out not to run
coordination at all — is reported as `{"capability_missing": true, "capability": "coordination",
"error": "..."}` instead: it declined correctly, it is not down, and conflating the two shapes
would send an operator investigating a healthy remote for an outage that never happened.
`mempalace_coordination_event_get` — a single audit event by exact ID — has no remote
counterpart at all: Stage 3 never exposed `GET /v1/coordination/events/{id}`, only the paginated
feed, so it stays local-only.

Being aggregate reads, these two never go through `resolve_coordination_route`, so they carry
their own guards rather than inheriting its — placed inside
`FederationRouter::coordination_inbox_fanout`/`coordination_events_fanout` themselves (both now
built on the shared `coordination_candidates()` iterator every candidate-narrowed loop in
`federation.rs` uses) so no future call site can add a fan-out read without them:

- **The same coordination opt-in gate as the ID-discovery fallbacks.** `has_remotes()` alone
  (true whenever *any* remote is configured for *anything* — drawers, KG, anything) is not
  enough to fan out a coordination read; the fan-out also requires
  `coordination_federation_enabled()` — an explicit `federation.coordination` entry, or
  `default_mode` itself non-`local`. A palace that federates drawers only, with an empty
  `federation.coordination` table and `default_mode: local`, never sends a recipient name or
  wing filter to any remote on `mempalace_inbox_read` or `mempalace_coordination_events` — a
  configured remote alone is not enough, exactly as for the ID-keyed fallbacks above.
- **Only coordination candidates are contacted at all**, per the candidate-set narrowing
  described above — a remote configured for drawers/KG only never receives a coordination fan-out
  query, regardless of `has_remotes()` or `coordination_federation_enabled()`.
- **`wing_agents` never reaches a remote through either feed.** When the requested `wing`
  normalises (via `WingId::normalized`, so `"agents"`/`"Wing_Agents"`/`" wing_agents "` all
  count) to `wing_agents`, both methods skip the remote fan-out entirely and return an empty
  result map — the local page still returns normally, just with no `remote_messages`/
  `remote_events` entries, the same shape a healthy config with zero configured remotes
  produces. A wing argument that fails to normalise at all fails CLOSED the same way
  (suppressed, not fanned out) rather than falling through un-filtered. A request with no
  `wing` argument at all is unaffected — there is no wing to protect against in an unfiltered
  aggregate read.

**Continuing a federated page uses `remote_cursors`, not `cursor`.** The local `cursor`
argument on `mempalace_inbox_read`/`mempalace_coordination_events` only advances the local
page. Each remote's own page is continued independently by echoing back its
`remote_messages.<name>.next_cursor` / `remote_events.<name>.next_cursor` from the previous
response inside a `remote_cursors: {"<name>": "<cursor>"}` object argument — the same
per-remote-map shape `mempalace_get_changes_since`'s `cursors` argument already uses. Treat
each value as opaque; do not parse it or reuse it against a different remote. `remote_messages`/
`remote_events` is an empty object whenever coordination federation is not configured for the
requested wing (including the diary wing) or no remotes are configured at all — an empty map
there does not mean pagination finished, only that no remote was queried in the first place.

**Revision conflicts cross the wire intact.** A `409 revision_conflict` from a remote — from a
`claim`/`renew`/`transition` discovered via the ID-fallback above — decodes into
`RemoteRevisionedWrite::Conflict { actual_revision }` rather than an `Err`, and the MCP tool
reports it via the same `{"success": false, "conflict": {"expected_revision": ..., "actual_revision": ...}}`
shape a local conflict already uses (see [Part 7 → Revision and lease
conflicts](#revision-and-lease-conflicts)). A `409 coordination_conflict` (a live lease held by
someone else, a terminal task, an invalid transition) has no revision pair to report and stays
a hard error — MemPalace never retries a conflicting write on the caller's behalf, locally or
federated.

### Discovering coordination routing

Every routing decision documented above except one can be inspected without side effects:
drawer routing shows up in `wing_availability`, configured remotes show up in
`mempalace_status`, and `/v1/info` lists capabilities. Coordination routing was the exception —
the only way to learn where `mempalace_task_create` would put a task in a given wing was to
create one, and coordination has no delete. Issue #125 closes that gap with a
`coordination_availability` map, a sibling to `wing_availability` rather than a change to it,
returned by the same four discovery tools whenever federation has remotes configured:

```jsonc
{
  "wing_availability":         { "wing_code": "combined", "wing_myproject": "local" },
  "coordination_availability": { "wing_code": "remote:work", "wing_myproject": "local", "wing_tasks": "remote:work" }
}
```

- `mempalace_status`, `mempalace_list_wings`, `mempalace_list_rooms`, and `mempalace_get_taxonomy`
  all emit it, exactly like `wing_availability`.
- Each entry is resolved by calling `resolve_coordination_route` directly — the same function
  `mempalace_task_create` uses — never a second implementation of its precedence. That
  precedence has already produced three separate bugs in this file when re-derived on a parallel
  code path (the coordination opt-in gate, the diary override, and the candidate-set narrowing
  each landed on one of two paths and missed the other — see the deviations in
  [Coordination-Phase-3-Design.md](Coordination-Phase-3-Design.md)), so the availability map
  reuses the wrapper instead of re-deriving the chain.
- The key set is the union of the local wing names, `federation.wings`, and
  `federation.coordination`. Drawer and coordination routing are deliberately separate tables
  (see [`FederationConfigV1::coordination`](../crates/mempalace-config/src/federation.rs) for
  why), so the same wing can legitimately answer differently in each map — `wing_code` above is
  `combined` for drawers but `remote:work` for coordination. Including `federation.wings` in the
  key set (not just `federation.coordination`) means a wing configured only for drawers still
  gets a coordination answer (it falls through to `default_mode`), and `wing_tasks` above — named
  only in `federation.coordination`, holding no drawers — is visible at all, which
  `wing_availability`'s key set never allowed.
- `wing_agents` always reports `"local"` in `coordination_availability`, unconditionally — the
  same hard override `resolve_coordination_route` applies everywhere else, not a special case in
  the availability map itself.
- Choosing a destination explicitly at `task_create` time, rather than discovering it here, is
  a related but separate capability and is not part of this change.

## Troubleshooting

### `remote '<name>' is unreachable ... writes do not fall back to local`
The hub is down or the URL/port is wrong. Federated writes with `write: remote`
intentionally do not fall back — fix connectivity or switch the wing to a
different target. For `write: both`, the local write still succeeds; check the
`replication` field on the MCP tool response for the failure detail.

### `remote '<name>' does not support the ingest capability`
The hub is an older build without `POST /v1/ingest/batch`. Upgrade the server.

### `remote '<name>' does not support the 'coordination' capability`
The hub is an older build without the `/v1/coordination/*` routes (pre-issue-#102-Stage-3).
Every coordination MCP tool checks this before sending a request — see [Client-side
coordination routing](#client-side-coordination-routing) — and fails with this error rather
than a confusing 404. Upgrade the server.

### `federation.coordination.<wing> may not use write: both`
A hard config-load error: coordination cannot be dual-written to two palaces because a task is
authoritative in exactly one (see the no-multi-master non-goal in
[Coordination-Phase-3-Design.md](Coordination-Phase-3-Design.md)). Use `write: local` or
`write: remote` on the `federation.coordination` entry instead — the identical `write: both`
setting on the corresponding `federation.wings` entry, for drawers, is unaffected and stays
legal.

### `federation.coordination.<key> is not in canonical wing form`
A hard config-load error: `federation.coordination` keys must already be in canonical `wing_*`
form (trimmed, lowercased, `wing_`-prefixed) — the error names the canonical spelling to use.
This is deliberately not auto-corrected: a non-canonical key never matched any resolved,
normalised wing before this check existed, so it was already inert (silently falling through to
`default_mode`), and silently rewriting it would activate a rule that was dead in the config as
written. Fix the key to its canonical form. `federation.wings` keys are not checked this way.

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

### A coordination write returns 409 with code `revision_conflict`
The `expected_revision` you sent no longer matches the task's current revision —
another writer moved it since you last read it. The response body's
`actual_revision` field is the current value; `GET` the task (or use it directly)
and retry with that revision if the retry is still appropriate. See
[Part 7 → Revision and lease conflicts](#revision-and-lease-conflicts).

### A coordination write returns 409 with code `coordination_conflict`
The write is not permitted regardless of revision: another worker's lease on the
task has not expired, the task is in a terminal state, the requested state
transition is invalid, or you are not the task's current owner. Retrying with a
fresher revision will not change the outcome — wait for the lease to expire (or
have the current owner release it) before claiming, or re-check the task's state
before transitioning. See [Part 7 → Revision and lease conflicts](#revision-and-lease-conflicts).

### A coordination route returns 404 for a task/message/artifact/result I know exists
Your token's `scopes` do not grant it visibility into that record's wing, or the
record lives in `wing_agents` (see [Part 7 → Wing is the authorization
key](#wing-is-the-authorization-key)). Per design this looks identical to the
record genuinely not existing, so check the token's `scopes` entry for the wing in
question rather than assuming the ID is wrong.

### A coordination write returns 409 with code `idempotency_key_conflict`
You replayed an `idempotency_key` your token has used before, but the record
storage originally created for it lives in a wing your token cannot currently see
— most often because its `scopes` narrowed since the original write, or because
the same key was reused across two different wings. The response deliberately
does not say which wing or repeat any of the record's content. See [Part 7 → Wing
is the authorization key](#wing-is-the-authorization-key). Use a distinct
`idempotency_key` per wing to avoid this.

### A coordination claim/renew returns 400 for `lease_seconds`
`lease_seconds` must be a positive integer no greater than roughly 100 years'
worth of seconds. See [Part 7 → Lease and expiry clocks belong to the palace that
owns the task](#lease-and-expiry-clocks-belong-to-the-palace-that-owns-the-task).

### A coordination write returns 400 for a claimed actor field
A `created_by`/`sender`/`worker`/`actor` claim containing `:` is rejected — that
character is reserved for the `identity:claimed` encoding the server builds when
a claim disagrees with the authenticated token identity. See [Part 7 → Actor
identity is derived, not claimed](#actor-identity-is-derived-not-claimed).
