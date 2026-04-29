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

Primary path:

- `<project>/mempalace.yaml`

Legacy fallback path accepted by the loader:

- `<project>/mempal.yaml`

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
