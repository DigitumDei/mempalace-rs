# Rust CLI Surface Freeze

This is the frozen command surface for `mempalace-cli` v1.

## Global Flag

- `--palace <PATH>`
  Overrides the palace path for the current invocation. During `init`, this also updates the global `config.json` palace path.

## Commands

### `init <dir>`

Purpose:
- Detect rooms from the project folder structure.
- Create or overwrite `mempalace.yaml` in the target project.
- Initialize the default global config tree if needed.
- Run embedding startup validation and report the status.

Flags:
- `--yes`
  Overwrite an existing `mempalace.yaml`.

Notes:
- Wing name is derived from the directory name, lowercased with spaces and hyphens normalized to underscores.
- Room detection is folder-name-based and always includes a `general` room.

### `mine <dir>`

Purpose:
- Ingest project files or conversation exports into the palace.

Flags:
- `--mode <projects|convos>`
- `--wing <STRING>`
- `--agent <STRING>` default: `mempalace`
- `--limit <N>` default: `0`, meaning no explicit limit
- `--dry-run`
- `--extract <exchange|general>` default: `exchange`
- `--reindex`
  Re-process files that were previously ingested and are unchanged on disk by bypassing the unchanged-content skip. In `projects` mode this converts existing content rows to locator rows — use it as the one-time migration step after upgrading a palace from pre-locator storage.
- `--branch`
  Mine only files changed vs the merge-base with the default branch (plus untracked files). Always writes to the local palace regardless of federation routing. Uses the `projects-branch` source-key namespace so branch rows never collide with a full mine. Unsupported for `--mode convos`.
- `--batch-size <N>` default: unset
  Largest batch to process at once; lower it to bound peak memory and CPU on low-spec machines. For a local mine it caps the number of chunks embedded per batch (default: a file's chunks are embedded together). For a remote-routed mine it caps the number of files per `POST /v1/ingest/batch` request (default: `64`); the ~4 MiB per-request byte cap still applies as an independent guardrail. `0` or omitted keeps the defaults.

Behavior:
- `projects` uses the project ingest path.
- `convos` uses the conversation ingest path.
- In low-CPU mode, ingest batching is clamped by the resolved low-CPU runtime config. An explicit `--batch-size` overrides that clamp (it takes precedence over the low-CPU default).
- `--reindex` bypasses the unchanged-content skip in both `projects` and `convos` modes.
- When the wing's federation route targets a remote palace (mode `remote`, or mode `combined` with `write: remote`) and `--branch` is not set, the CLI prepares chunks locally and pushes them to `POST /v1/ingest/batch` on the remote server. The remote must advertise the `"ingest"` capability in `GET /v1/info`; older servers that lack this endpoint return a 404, which surfaces as a `RemoteRejected` error with a prompt to upgrade.
- `--branch` overrides any remote route for the wing — branch-delta mining is always local.

### `search <query>`

Purpose:
- Semantic retrieval with optional wing and room filters.

Flags:
- `--wing <STRING>`
- `--room <STRING>`
- `--results <N>` default: `5`

Behavior:
- In low-CPU mode, the requested result count is clamped to the effective low-CPU search limit.
- Search fails with a non-zero result if no palace exists at the resolved palace path.

### `status`

Purpose:
- Show wing and room drawer counts from the current palace.

Behavior:
- Returns a non-zero result with guidance if no palace exists.

### `wake-up`

Purpose:
- Render L0 + L1 wake-up context for the whole palace or a single wing.

Flags:
- `--wing <STRING>`

Behavior:
- Default L1 assembly uses the search crate default and is then clamped by low-CPU limits when enabled.
- If no palace exists, the command returns a non-zero result with the expected bootstrap guidance.

### `serve`

Purpose:
- Run the federation HTTP server over the current palace, exposing it to remote
  clients via the REST API. See the [Federation guide](Federation.md) for the full
  setup.

Flags:
- `--bind <ADDR>`
  Socket address to listen on, e.g. `127.0.0.1:8765`. Default: `server.bind` from
  `config.json`, falling back to `127.0.0.1:8765`.
- `--token-file <PATH>`
  Path to the bearer-token JSON file. Default: `server.token_file` from
  `config.json`, falling back to `~/.mempalace/server_tokens.json`.

Behavior:
- The token file is a JSON array of objects, each with `token`, `name`, and
  `enabled` keys; it is hot-reloaded on each request, and tokens are hashed in
  memory.
- `GET /v1/health` is unauthenticated; all other `/v1` routes require
  `Authorization: Bearer <token>`.
- The server speaks plain HTTP and prints a warning to that effect — front it with
  TLS termination on untrusted networks.
- Honors `MEMPALACE_STUB_EMBEDDINGS` (deterministic stub provider) for offline dev
  testing.
- Runs until interrupted; shuts down gracefully on Ctrl-C.

### Deferred Commands

These commands are intentionally visible but not shipped as working Rust v1 functionality:

- `split`
- `compress`

Each returns a non-zero result and points at [Phase09-Deferred-Commands](../rust-phase-plans/Phase09-Deferred-Commands.md).

## Exit Behavior

- Successful command execution returns exit code `0`.
- Deferred-command and missing-palace flows return a non-zero result with explicit guidance.
- Clap parse failures still use Clap's normal non-zero error flow.
