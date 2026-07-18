# Rust Config Schema Freeze

This document freezes the config and runtime override surface used by `mempalace-rs` v1.

## Global Config File

Path:

- default: `~/.mempalace/config.json`

Schema version:

- `version: 1`

Frozen JSON shape:

```json
{
  "version": 1,
  "palace_path": "~/.mempalace/palace",
  "collection_name": "mempalace_drawers",
  "embedding_profile": "balanced",
  "low_cpu": {
    "worker_threads": 1,
    "max_blocking_threads": 1,
    "queue_limit": 32,
    "ingest_batch_size": 8,
    "search_results_limit": 5,
    "wake_up_drawers_limit": 8,
    "degraded_mode": true,
    "rerank_enabled": false
  }
}
```

Notes:

- `low_cpu` is optional.
- If the config file does not exist, Rust defaults are used and `init` will create the file.
- `version != 1` is rejected.

## Field Definitions

### `version`

- Type: integer
- Required for persisted config
- Supported value in v1: `1`

### `palace_path`

- Type: string
- Optional in file, resolved to `~/.mempalace/palace` by default
- `~/...` expansion is supported

### `collection_name`

- Type: string
- Default: `mempalace_drawers`

### `embedding_profile`

- Type: string enum
- Allowed values:
  - `balanced`
  - `low_cpu`
- Default: `balanced`

### `low_cpu`

- Type: object
- Optional
- Fields:
  - `worker_threads`
  - `max_blocking_threads`
  - `queue_limit`
  - `ingest_batch_size`
  - `search_results_limit`
  - `wake_up_drawers_limit`
  - `degraded_mode`
  - `rerank_enabled`

Validation:

- `worker_threads`, `max_blocking_threads`, and `ingest_batch_size` must be greater than `0` when set.
- Limit fields may be set to `0`; the runtime treats that literally and does not remap it to a default.

## Profile Defaults

### `balanced`

- `embedding_profile = "balanced"`
- low-CPU mode disabled
- search and wake-up limits are effectively unbounded by low-CPU clamps

### `low_cpu`

- `embedding_profile = "low_cpu"`
- runtime defaults:
  - `worker_threads = 1`
  - `max_blocking_threads = 1`
  - `queue_limit = 32`
  - `ingest_batch_size = 8`
  - `search_results_limit = 5`
  - `wake_up_drawers_limit = 8`
  - `degraded_mode = true`
  - `rerank_enabled = false`

Degraded effective clamps:

- `queue_limit <= 8`
- `ingest_batch_size <= 4`
- `search_results_limit <= 3`
- `wake_up_drawers_limit <= 4`

If `degraded_mode = false`, the configured non-degraded values apply directly.

## Environment Overrides

Supported environment variables:

- `MEMPALACE_PALACE_PATH`
- `MEMPAL_PALACE_PATH`
  Legacy alias retained for Python-era compatibility.
- `MEMPALACE_EMBEDDING_PROFILE`

Override order:

1. Explicit CLI `--palace`
2. Environment override
3. `config.json`
4. Built-in default

## Project Config File

Repository-local compatibility path:

- `<project>/mempalace.yaml`

Legacy fallback path accepted by the loader:

- `<project>/mempal.yaml`

Repository-local files are optional. The default CLI workflow stores project
declarations centrally at `<base-dir>/projects.json` (normally
`~/.mempalace/projects.json`) so clones and worktrees can share one mapping.

The registry is keyed by a normalized Git origin when available and stores the
wing, room rules, optional federation route, and checkout-path aliases. A
registry entry has the same project fields as the YAML shape below, plus
`checkouts` and an optional `project_root` for monorepos:

```json
{
  "version": 1,
  "projects": {
    "github.com/digitumdei/mempalace-rs": {
      "wing": "wing_mempalace_rs",
      "checkouts": ["D:/SourceCode/mempalace-rs"],
      "rooms": [
        {"name": "crates", "description": "Rust crates", "keywords": ["rust"]}
      ],
      "routing": {"mode": "local"},
      "project_root": null
    }
  }
}
```

Resolution order is: explicit CLI selection/wing, repository-local YAML (when
present), the centralized registry, then derived defaults. Existing YAML files
remain supported as portable overrides and for backward compatibility. `init`
and `project register` import an existing YAML declaration into the registry
without modifying the repository file. Use `--repo-config` when you explicitly
want a file written; `--yes` is required when that would replace an existing
file. Monorepo entries use `<origin>#<project-root>` IDs, while checkout paths
are aliases only. Ambiguous aliases or subproject mappings are reported as
conflicts.

Frozen YAML shape:

```yaml
wing: my_project
rooms:
  - name: backend
    description: Files from api/
    keywords:
      - backend
      - api
  - name: general
    description: Files that don't fit other rooms
    keywords: []
```

Fields:

- `wing`: required string
- `rooms`: list of room objects
- room object fields:
  - `name`: required string
  - `description`: optional string
  - `keywords`: optional string list

### Project-level routing block

An optional `routing` block in `mempalace.yaml` sets a default route for the wing declared in that file:

```yaml
wing: wing_myproject
routing:
  mode: combined
  remote: work
  write: local
```

- `mode`: `local`, `remote`, or `combined`
- `remote`: name of a remote defined in `~/.mempalace/config.json` federation.remotes. May be omitted when exactly one remote is configured.
- `write`: `local`, `remote`, or `both`. Only meaningful for `combined` mode. Default: `local`.
  - `both` performs a local-first dual-write: the local write must complete
    successfully, then best-effort remote replication is attempted. Remote
    failure does not roll back the local write or change the success result.
    The outcome of the remote leg is reported as a `replication` field on MCP
    tool responses — see [`ReplicationStatus`](#replicationstatus).

## Server Config

The optional `server` section of `~/.mempalace/config.json` configures the
federation HTTP server started by `mempalace serve`.

### Shape

```jsonc
{
  "server": {
    "bind": "127.0.0.1:8765",
    "token_file": "~/.mempalace/server_tokens.json",
    "checkouts": {
      "wing_myproject": "/home/user/repos/myproject",
      "wing_teamdocs":  "/home/user/repos/teamdocs"
    }
  }
}
```

### Field Definitions

#### `server.bind`

- Type: string (`host:port` socket address)
- Optional. Default: `"127.0.0.1:8765"`
- Invalid socket-address strings fail config load with a precise error.

#### `server.token_file`

- Type: string (path, `~/`-prefixed strings are expanded)
- Optional. Default: `~/.mempalace/server_tokens.json`

#### `server.checkouts`

- Type: object mapping wing name → absolute checkout path
- Optional. Default: empty
- Used by `POST /v1/ingest/batch` to fill the `resolve_root` field of locator
  rows for mined files pushed from remote clients.
- When a wing is present in the map, snippet text is resolved from the
  configured path at search time, giving fresh locator results.
- When a wing is **absent** from the map (or the `checkouts` field is omitted
  entirely), the server stores locator rows with an empty `resolve_root`. Every
  search result for that wing resolves as a stale placeholder, and the batch
  response `warnings` array contains:
  `"no checkout configured for wing '<w>'; locator results will resolve as stale placeholders until server.checkouts is set"`
- Only the server that receives `POST /v1/ingest/batch` reads this field;
  clients that push batches do not need it.

## Federation Config

The optional `federation` section of `~/.mempalace/config.json` controls routing of wing reads and writes to remote palace servers.

### Shape

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
      "wing_teamdocs": { "mode": "remote", "remote": "work" },
      "wing_bigrepo":  { "mode": "combined", "remote": "work", "write": "local" },
      "wing_shared":   { "mode": "combined", "remote": "work", "write": "both" }
    },
    "kg": { "mode": "combined", "remote": "work", "write": "remote" }
  }
}
```

### Field Definitions

#### `federation.remotes`

- Type: array of remote objects
- Optional; defaults to empty
- Each remote object:
  - `name`: unique string identifier referenced by routing rules and `remote` fields elsewhere
  - `url`: must be an `http://` or `https://` URL — any other scheme fails config load
  - `token`: inline bearer token string (optional)
  - `token_env`: name of an environment variable holding the bearer token (optional, preferred over `token` — keeps secrets out of config.json)
  - Token resolution: the environment variable value wins if both are set; if `token_env` is set but the variable is not present in the environment, the config loader warns and falls back to the inline `token` (or proceeds unauthenticated if neither is set)
  - `timeout_ms`: HTTP request timeout in milliseconds. Default: `5000`

Validation:
- Duplicate `name` values across remotes fail config load.
- `url` must parse as a valid `http` or `https` URL; other schemes fail config load.

#### `federation.default_mode`

- Type: string enum: `local` | `remote` | `combined`
- Optional. Default: `local`
- `remote` or `combined` requires exactly one remote to be configured, or each routing rule must supply an explicit `remote` name; a missing or ambiguous remote reference fails config load.

#### `federation.wings`

- Type: object mapping wing name → routing rule
- Optional
- Each routing rule:
  - `mode`: `local` | `remote` | `combined` (required)
  - `remote`: name of an entry in `federation.remotes`. May be omitted when exactly one remote is configured; required (and fails config load if missing) when multiple remotes are configured.
  - `write`: `local` | `remote` | `both`. Only meaningful for `combined` mode. Default: `local`.
    - `local` — writes go to the local palace only.
    - `remote` — writes go to the remote palace only.
    - `both` — local-first dual-write; the local write must complete first,
      then best-effort remote replication is attempted. See
      [`ReplicationStatus`](#replicationstatus) for the response shape.

#### `federation.kg`

- Type: routing rule (same shape as a wings entry)
- Optional
- Controls knowledge-graph read/write routing independently of wing routing.

### Resolution Precedence

Routes are resolved in this order (first match wins):

1. Explicit per-wing rule in `federation.wings`
2. Project `mempalace.yaml` `routing` block for the wing declared in that file
3. `federation.default_mode`
4. `local` (hard default when no federation config is present)

**Hard overrides — always local regardless of config:**
The following are unconditionally resolved to local storage; any config rule that attempts to route them remote is warned about and ignored:
- Wing `wing_agents`
- Room `diary` (within any wing)
- Any source whose name begins with `diary:` prefix

### Validation Errors vs. Warnings

Config load fails with a precise error message for:
- Unknown remote reference in a routing rule (name not present in `federation.remotes`)
- Duplicate remote names in `federation.remotes`
- Non-`http`/`https` URL in a remote definition
- `remote` field missing or ambiguous when `mode` is `remote` or `combined` and multiple remotes are configured

Config load succeeds with a warning (does not fail) for:
- `token_env` set but the named environment variable is absent at startup
- `write` set on a rule whose `mode` is not `combined` (ignored)
- A non-local rule for `wing_agents` (ignored; the diary hard override applies at resolution time)

### `ReplicationStatus`

When `write: both` is configured, MCP tool responses carry a `replication` field
reporting the result of the best-effort remote leg. Non-`both` routes and
diary-local writes omit the field entirely. The shape is a tagged union:

```json
{ "status": "replicated", "remote": "work" }
```

```json
{ "status": "failed", "remote": "work", "reason": "transport failure: timeout" }
```

```json
{ "status": "converged", "remote": "work" }
```

| Variant | Meaning |
|---|---|
| `replicated` | Best-effort remote write to the named remote succeeded. |
| `converged` | Exact content already exists on the named remote; state is converged. |
| `failed` | Best-effort remote write failed; `reason` contains a human-readable description. The local write was unaffected. |

The `replication` field is absent when federation is not configured, the route
does not use `write: both`, or the write targets a diary-local destination.
