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
  },
  "maintenance": {
    "enabled": true,
    "background_enabled": true,
    "idle_secs": 300,
    "version_retention_hours": 24,
    "tail_threshold_rows": 1024,
    "small_fragment_threshold": 10
  }
}
```

Notes:

- `low_cpu` is optional.
- `maintenance` is optional.
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

### `maintenance`

- Type: object
- Optional
- Defaults when absent or unset:
  - `enabled: true`
  - `background_enabled: true`
  - `idle_secs: 300`
  - `version_retention_hours: 24`
  - `tail_threshold_rows: 1024`
  - `small_fragment_threshold: 10`
- Fields:
  - `enabled`: boolean — master switch for all maintenance. When `false`, both the HTTP scheduler and `mempalace-cli maintain` are disabled. Default: `true`.
  - `background_enabled`: boolean — whether the HTTP server schedules maintenance automatically. Default: `true`. Set to `false` for low-I/O operation; `mempalace-cli maintain` remains available for a planned maintenance window when `enabled` remains `true`.
  - `idle_secs`: positive integer — minimum idle seconds since the last write before maintenance runs. Default: `300`.
  - `version_retention_hours`: positive integer — maximum age in hours for retained version data. Default: `24`.
  - `tail_threshold_rows`: positive integer — row count threshold that triggers incremental vector-index optimization. Default: `1024`.
  - `small_fragment_threshold`: positive integer — number of small LanceDB fragments that trigger fragment compaction. Default: `10`.

Validation:

- `idle_secs`, `version_retention_hours`, `tail_threshold_rows`, and `small_fragment_threshold` must be greater than `0` when set in the config file. Zero-valued env overrides are also rejected.

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
- `MEMPALACE_MAINTENANCE_ENABLED`
  Overrides `maintenance.enabled`. Accepted true values: `1`, `true`, `TRUE`, `yes`, `YES`. Accepted false values: `0`, `false`, `FALSE`, `no`, `NO`. Other values are rejected.
- `MEMPALACE_MAINTENANCE_BACKGROUND_ENABLED`
  Overrides `maintenance.background_enabled`. It accepts the same true and false values as `MEMPALACE_MAINTENANCE_ENABLED`.
- `MEMPALACE_MAINTENANCE_IDLE_SECS`
  Overrides `maintenance.idle_secs`. Must be a positive integer. Zero is rejected.
- `MEMPALACE_MAINTENANCE_VERSION_RETENTION_HOURS`
  Overrides `maintenance.version_retention_hours`. Must be a positive integer. Zero is rejected.
- `MEMPALACE_MAINTENANCE_TAIL_THRESHOLD_ROWS`
  Overrides `maintenance.tail_threshold_rows`. Must be a positive integer. Zero is rejected.
- `MEMPALACE_MAINTENANCE_SMALL_FRAGMENT_THRESHOLD`
  Overrides `maintenance.small_fragment_threshold`. Must be a positive integer. Zero is rejected.

Override order:

1. Explicit CLI `--palace`
2. Environment override
3. `config.json`
4. Built-in default

### Other environment variables

These are read directly by the runtime or by examples and have no `config.json`
counterpart, so they are not part of the override chain above:

| Variable | Read by | Effect |
|---|---|---|
| `MEMPALACE_EMBED_ALLOW_DOWNLOADS` | `mempalace-cli`, `mempalace-mcp`, `mempalace-cli serve` | Permits downloading missing embedding assets. Offline is the default. |
| `MEMPALACE_STUB_EMBEDDINGS` | `mempalace-mcp`, `mempalace-cli serve` — **only** | Selects a deterministic stub embedding provider for offline dev and testing. |
| `MEMPALACE_BUILD_VERSION` | build script | Embeds the calculated release version in both binaries and in `GET /v1/info`. Unset falls back to the workspace package version. See [Release Operations](Release-Operations.md#release-versions). |
| `MEMPALACE_EMBED_CACHE` | `embedding_bench` / `lme_bench` examples | Overrides the embedding cache root for benchmark runs only. |
| `MEMPALACE_EMBED_PROFILE`, `MEMPALACE_EMBED_ITERATIONS` | `embedding_bench` example | Benchmark profile selection and iteration count. |

The two embedding flags are parsed by the same helper (`env_flag`) and accept only an
explicit truthy value — `1`, `true`, `TRUE`, `yes`, `YES`. Every other value is false,
including `0`, `false`, and the empty string, so `MEMPALACE_STUB_EMBEDDINGS=0` disables stub
vectors rather than enabling them.

> **`MEMPALACE_STUB_EMBEDDINGS` reaches only the two long-running servers.** The CLI consults
> it inside `serve` alone; `init`, `mine`, and `search` always construct the real
> `FastembedProvider`, so on a host with no model cache they still fail with missing assets
> however the variable is set.

> **Don't leave `MEMPALACE_STUB_EMBEDDINGS` set in an environment that expects real
> embeddings.** Stub vectors are deterministic placeholders and are not comparable with
> model output, so a palace written while it is enabled returns misleading search results.

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
- The token file is a JSON array of objects, each with:
  - `token`: string — the bearer secret a client must present (required).
  - `name`: string — the identity recorded as `added_by` on writes from that token (required).
  - `enabled`: boolean — `false` treats the entry as if it did not exist (instant revoke) (required).
  - `level`: string — optional access level. One of `read`, `write`, or `admin`. Defaults to `write` when omitted (backward compatible). Unknown values fail server start.
- Access is enforced per route: `read` tokens may call every read route but are rejected with `403 forbidden` on write routes; `write` and `admin` are equivalent for the current route set. The level is a whole-token gate, not a per-user or per-wing policy.

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
