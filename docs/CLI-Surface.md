# Rust CLI Surface Freeze

This is the frozen command surface for `mempalace-cli` v1.

## Global Flag

- `--palace <PATH>`
  Overrides the palace path for the current invocation. During `init`, this also updates the global `config.json` palace path.

## Commands

### `init <dir>`

Purpose:
- Detect rooms from the project folder structure.
- Register the project centrally under the configured MemPalace base directory.
- Initialize the default global config tree if needed.
- Run embedding startup validation and report the status.

Flags:
- `--yes`
  Permit replacing an existing repository-local config when `--repo-config` is
  also supplied.
- `--repo-config`
  Also write a portable repository-local `mempalace.yaml`. Without this flag,
  `init` does not modify repository files.
- `--project-id <STRING>`
  Use an explicit durable identity when the repository has no usable Git
  origin. The same option is available on `mine` and `project register`.

Notes:
- Wing name is derived from the directory name, lowercased with spaces and hyphens normalized to underscores.
- Room detection is folder-name-based and always includes a `general` room.
- The central registry is stored at `<base-dir>/projects.json` (normally
  `~/.mempalace/projects.json`) and uses normalized Git origin identity when
  available, with checkout paths as discovery aliases.

### `mine <dir>`

Purpose:
- Ingest project files or conversation exports into the palace.

Flags:
- `--mode <projects|convos>`
- `--wing <STRING>`
- `--project-id <STRING>`
  Select a centralized declaration explicitly. This takes precedence over a
  repository-local compatibility file.
- `--agent <STRING>` default: `mempalace`
- `--limit <N>` default: `0`, meaning no explicit limit
- `--dry-run`
- `--extract <exchange|general>` default: `exchange`
- `--reindex`
  Re-process files that were previously ingested and are unchanged on disk by bypassing the unchanged-content skip. In `projects` mode this converts existing content rows to locator rows — use it as the one-time migration step after upgrading a palace from pre-locator storage.
- `--branch`
  Mine only files changed vs the merge-base with the default branch (plus untracked files). Always writes to the local palace regardless of federation routing. Uses the `projects-branch` source-key namespace so branch rows never collide with a full mine; keys include stable project identity and branch name. Unsupported for `--mode convos`.
- `--batch-size <N>` default: unset
  Largest batch to process at once; lower it to bound peak memory and CPU on low-spec machines. For a local mine it caps the number of chunks embedded per batch (default: a file's chunks are embedded together). For a remote-routed mine it caps the number of files per `POST /v1/ingest/batch` request (default: `64`); the ~4 MiB per-request byte cap still applies as an independent guardrail. `0` or omitted keeps the defaults.

Behavior:
- `projects` uses the project ingest path.
- `convos` uses the conversation ingest path.
- Project resolution checks explicit CLI values, optional repository-local
  config, the central project registry, and then derived defaults. A project
  can therefore be mined without `mempalace.yaml`.
- In low-CPU mode, ingest batching is clamped by the resolved low-CPU runtime config. An explicit `--batch-size` overrides that clamp (it takes precedence over the low-CPU default).
- `--reindex` bypasses the unchanged-content skip in both `projects` and `convos` modes.
- When the wing's federation route targets a remote palace (mode `remote`, or mode `combined` with `write: remote`) and `--branch` is not set, the CLI prepares chunks locally and pushes them to `POST /v1/ingest/batch` on the remote server. The remote must advertise the `"ingest"` capability in `GET /v1/info`; older servers that lack this endpoint return a 404, which surfaces as a `RemoteRejected` error with a prompt to upgrade.
- When the wing's federation route is `combined` with `write: both` and `--branch` is not set, the CLI performs a **local-first dual-write**: the full local mine (embedding, storage, summary) runs first, then a best-effort remote push is attempted. The remote result is appended to the mine output; a remote failure is reported without rolling back the local mine. See [Federation guide](Federation.md#write-both--local-first-dual-write-semantics) for the full semantics.
- `--branch` overrides any remote route for the wing — branch-delta mining is always local.

### `project <register|show|list|remove|export>`

Purpose:
- Inspect and manage centralized project declarations.

Commands:
- `project register <dir> [--wing <STRING>]` registers or updates a project
  using existing repository rules when present, otherwise detected room rules.
- `project register --project-id <STRING>` supplies an explicit identity for a
  repository without an origin.
- `project show <dir>` displays the declaration resolved for a checkout.
- `project list` lists registered project identities and wings.
- `project remove <project-id>` removes one registry entry.
- `project export <project-id> [--dir <PATH>] --repo-config` writes the
  centralized declaration as a portable repository-local override.

`project register --repo-config` additionally emits a portable repository-local
`mempalace.yaml`.

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

### `setup`

Purpose:
- Detect which supported AI coding tools are installed and register the mempalace MCP server (`mempalace-mcp`) with each, idempotently.

Flags:
- `--dry-run`
  Preview what would change — print the command that would run / file that would be written for each detected tool — without running anything or writing any file.
- `--mcp-path <PATH>` default: `~/.mempalace/bin/mempalace-mcp` (`.exe` on Windows)
  Absolute path to the `mempalace-mcp` binary that tools are pointed at. A warning is printed if the binary is not present there yet (tools are still configured to launch it once installed).
- `--tools <LIST>` default: all
  Comma-separated subset of tool keys to limit setup to: `claude,codex,gemini,opencode,copilot,antigravity,jules`.

Behavior:
- Per-tool mechanism (verified against each tool's official docs):
  - **claude / codex / gemini** — registered via the tool's own CLI (`claude mcp add --scope user`, `codex mcp add`, `gemini mcp add -s user`), at user/global scope. Requires the tool's binary on `PATH`. Idempotent: an existing `mempalace` server is detected and left as-is. The binary is invoked by its resolved path (including the npm `.cmd` shim on Windows), so arguments — including the MCP path — are passed as real argv entries rather than re-parsed by `cmd.exe`.
  - **opencode** — merges a `mcp.mempalace` entry (`type: "local"`, command as a single-element array) into `~/.config/opencode/opencode.json` (XDG path, the same on Windows).
  - **copilot** — merges a `mcpServers.mempalace` entry into `~/.copilot/mcp-config.json`.
  - **antigravity** — merges a `mcpServers.mempalace` entry into both `~/.gemini/config/mcp_config.json` and `~/.gemini/antigravity-cli/mcp_config.json` (the config location differs across Antigravity versions; writing both is harmless). Detection keys off the antigravity-owned `~/.gemini/antigravity-cli/` directory (not the bare `~/.gemini/config/`, which is shared with the Gemini CLI).
  - **jules** — reported as unsupported and skipped: it is a cloud agent that only allows a curated set of remote MCP integrations configured in its web UI, so it cannot run a local stdio server.
- JSON merges preserve all other keys and are idempotent (re-running reports "already configured"). If an existing config file is not valid JSON, setup refuses to clobber it and reports a failure for that tool.
- Tools that are not installed are skipped with a note. The command always exits 0 (best-effort across tools); per-tool status is shown in the summary.

### `maintain`

Purpose:
- Run a single maintenance pass (compact, prune, optimize) against the current
  palace using the configured settings.  This is a one-shot CLI invocation
  intended for initial backfill on large existing palaces and for
  out-of-band troubleshooting; the HTTP hub (`serve`) runs maintenance
  automatically in the background.

Flags:
- (none beyond the global `--palace` flag)

Behavior:
- When maintenance is **disabled** in configuration, the command prints a
  message to that effect and exits 0.
- When maintenance is **enabled**, the command runs all three tiers:
  1. **Vector Index Optimization** — rebuilds LanceDB vector indices for
     faster ANN search.
   2. **Fragment Compaction** — merges small LanceDB fragments to reduce
      storage and improve scan performance.  Triggered when the number of
      small fragments exceeds `small_fragment_threshold` (default: 10).
  3. **Version Retention** — purges version rows older than
     `version_retention_hours` (default: 24).
- The one-shot CLI bypasses the process-local idle gate (`idle_secs` is
  treated as 0) so the pass runs immediately, but respects all other
  configured thresholds and the `enabled` flag.
- Cross-process lease coordination applies: the command acquires a SQLite
  advisory lease with a 5-minute TTL.  If another process (e.g. the hub
  or another CLI invocation) already holds the lease, the command exits
  with a "concurrent run" status rather than duplicating work.
- Prints a formatted summary including run ID, start/end timestamps,
  wall-clock and CPU duration, per-tier outcomes, and overall status.
- Exit codes:
  - `0` — all tiers completed successfully, or at least one tier was
    skipped (non-critical).  Also `0` when maintenance is disabled.
  - `1` — at least one tier failed or was aborted.

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
