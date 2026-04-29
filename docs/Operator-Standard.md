# Standard Operator Guide

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

If the MCP host needs to bootstrap a cold cache on first start, launch it with:

```bash
MEMPALACE_EMBED_ALLOW_DOWNLOADS=1 target/release/mempalace-mcp
```

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
