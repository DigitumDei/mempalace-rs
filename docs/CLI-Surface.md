# Rust CLI Surface Freeze

This is the frozen command surface for `mempalace-cli` v1.

## Global Flag

- `--palace <PATH>`
  Overrides the palace path for the current invocation. During `init`, this also updates the global `config.json` palace path.

## Commands

### `init <dir>`

Purpose:
- Detect rooms from the project's safe source directories.
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
- Room detection derives from the same safe source set mining uses: Git-backed
  checkouts use the tracked index, and non-Git directories use the
  ignore-aware filesystem walk, so ignored, untracked, secret-shaped,
  tracked-symlink, and linked-worktree files never produce rooms. A Git-backed
  root whose index
  cannot be enumerated fails discovery rather than silently falling back to a
  filesystem walk. A `general` room is always included.
- The summary's file count reports the number of eligible project sources in
  that same safe set — the files `mine` would actually ingest — rather than a
  raw directory traversal, so ignored/untracked/secret-shaped files are not
  counted.
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
  Force a branch-delta mine: only files changed vs the merge-base with the default branch (plus untracked files). Always writes to the local palace regardless of federation routing. Uses the `projects-branch` source-key namespace so branch rows never collide with a canonical mine; keys include stable project identity and the view name. Unsupported for `--mode convos`. Conflicts with `--full` and with `--view canonical`.
- `--view <NAME>`
  Explicit view/ref name for this mine, overriding automatic detection. `--view canonical`
  forces a full canonical mine and is equivalent to `--full`. Any other value mines a branch
  delta stored under that view name.
- `--full`
  Force a full canonical mine, ignoring automatic branch detection. Equivalent to
  `--view canonical`. Conflicts with `--view`.
- `--batch-size <N>` default: unset
  Largest batch to process at once; lower it to bound peak memory and CPU on low-spec machines. For a local mine it caps the number of chunks embedded per batch (default: a file's chunks are embedded together). For a remote-routed mine it caps the number of files per `POST /v1/ingest/batch` request (default: `64`); the ~4 MiB per-request byte cap still applies as an independent guardrail. `0` or omitted keeps the defaults.

Automatic view detection (`--mode projects` only):

- The checkout is classified before mining:
  - **Canonical** — HEAD is on the repository's default branch → full canonical mine.
  - **Branch** — HEAD is on any other branch, or detached (view name
    `detached-<12-hex>` derived from the repository toplevel path) → branch-delta mine
    under that view name.
  - **Non-Git**, or a repository with no resolvable default branch → full mine, preserving
    pre-view behaviour.
- `--full` / `--view canonical` force canonical; `--branch` / `--view <name>` force a branch
  delta. An explicit selector always wins over detection.
- An **automatically** detected branch mine requires an existing canonical snapshot for the
  project, and the lookup queries the **local** palace only. This guard does not apply when
  `--branch` or `--view` was passed explicitly. If no local snapshot exists the command
  exits non-zero with `automatic branch mining requires an existing canonical snapshot`,
  followed by recovery advice that depends on the wing's route:
  - **Local wing** — `mine the canonical checkout first or use --full to intentionally
    replace it`.
  - **Wing whose canonical mines route to a remote** (`mode: remote`, or `combined` with
    `write: remote`) — ``wing `<name>` routes canonical mines to a remote, so --full cannot
    create the local snapshot this check needs; pass --branch or --view <name> to mine the
    branch delta deliberately``. `--full` is not a fix there: it is a canonical mine, so it
    would push to the remote and never create the local snapshot the guard wants.
  - `combined` with `write: both` gets the local-wing message, because that route still
    performs the full local mine and so does leave a local snapshot behind.

  See [Federation guide](Federation.md#part-4--branch-aware-mining).
- The resolved view name is echoed in the mine summary as `View: <name>`; canonical mines
  print no `View` line.

Behavior:
- `projects` uses the project ingest path.
- `convos` uses the conversation ingest path.
- In `projects` mode, source discovery honours git: a Git-backed root mines the
  tracked index only (`git ls-files`), so ignored and untracked working-tree
  files (e.g. `.env`, `*.local.json`, build output) are never ingested. A
  `.gitignore` never suppresses a tracked file; `.mempalaceignore` is the
  explicit additional exclusion. Tracked symlinks are rejected outright before
  any eligibility check or file read, so a link that escapes the discovery
  root can never pull its target's content into the palace. Independently of
  git, a path-based **secret
  denylist** (issue #95) withholds secret-shaped paths — `.env`/`*.env`,
  `*.kubeconfig*`, SSH private keys (`id_rsa*`, `id_ed25519`, `id_ecdsa`,
  `id_dsa`), keystores (`*.pfx`/`*.p12`/`*.jks`), `.npmrc`/`.netrc`,
  `*.tfstate`/`*.tfvars`, `secrets*.json`, `*.local.json` — before any content
  is read, in both Git-index and filesystem discovery. These are counted like
  any skipped candidate and shown in the mine summary as `Secrets withheld: N`
  with one `<path> — secret-shaped path (<reason>)` line per withheld file
  (never the file content). Non-Git directories fall back to a filesystem
  walk that honours `.gitignore` and `.mempalaceignore` at every level with
  git-compatible semantics (nesting, `!` negation, anchoring, and globs) plus
  the `core.excludesFile` global excludes file. Branch-delta mines
  (`--branch` / `--view <name>`) are the exception: they additionally include
  untracked, non-ignored files, and their filesystem walk honours
  `$GIT_DIR/info/exclude` too — both repository-level sources at git's
  precedence. Linked git worktrees are always excluded from mining. A
  Git-backed root whose index cannot be enumerated fails discovery with a
  `GitIndexUnavailable` error rather than falling back to a filesystem walk, so
  a git-read failure can never leak untracked or ignored content into a
  canonical mine. See
  [Mined-Storage.md#discovery-rules](Mined-Storage.md#discovery-rules).
- A canonical mine's `Files discovered` count is the same effective source
  population that `init` and `project register` report, so the three commands
  agree on what would be ingested. Branch-delta mines are the deliberate
  exception: their filesystem walk also picks up untracked, non-ignored files.
- Project resolution checks explicit CLI values, optional repository-local
  config, the central project registry, and then derived defaults. A project
  can therefore be mined without `mempalace.yaml`.
- In low-CPU mode, ingest batching is clamped by the resolved low-CPU runtime config. An explicit `--batch-size` overrides that clamp (it takes precedence over the low-CPU default).
- `--reindex` bypasses the unchanged-content skip in both `projects` and `convos` modes.
- When the wing's federation route targets a remote palace (mode `remote`, or mode `combined` with `write: remote`) and `--branch` is not set, the CLI prepares chunks locally and pushes them to `POST /v1/ingest/batch` on the remote server. The remote must advertise the `"ingest"` capability in `GET /v1/info`; older servers that lack this endpoint return a 404, which surfaces as a `RemoteRejected` error with a prompt to upgrade.
- When the wing's federation route is `combined` with `write: both` and `--branch` is not set, the CLI performs a **local-first dual-write**: the full local mine (embedding, storage, summary) runs first, then a best-effort remote push is attempted. The remote result is appended to the mine output; a remote failure is reported without rolling back the local mine. See [Federation guide](Federation.md#write-both--local-first-dual-write-semantics) for the full semantics.
- Branch-delta mining is always local. Any resolved branch view — whether from `--branch`, `--view <name>`, or automatic detection — overrides a remote route for the wing. Only canonical mines are eligible for federated batch ingest.

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

`project register` derives rooms from the same safe source set `init` and a
canonical mine use (tracked index for Git-backed roots, the ignore-aware
filesystem walk otherwise), and its output reports the same eligible source
count as `init`'s summary — the files a canonical mine would actually ingest —
so ignored, untracked, secret-shaped, tracked-symlink, and linked-worktree
files are neither counted nor turned into rooms.

### `search <query>`

Purpose:
- Semantic retrieval with optional wing and room filters.

Flags:
- `--wing <STRING>`
- `--room <STRING>`
- `--results <N>` default: `5`
- `--view <NAME>`
  Scope the search to a repository view. Omitted (or `canonical`) searches canonical
  snapshots and excludes branch views. A branch name composes that branch's changed paths
  over the canonical snapshot. `full` searches every stored repository view independently.

Behavior:
- In low-CPU mode, the requested result count is clamped to the effective low-CPU search limit.
- Search fails with a non-zero result if no palace exists at the resolved palace path.
- With a branch view selected, each canonical row whose `(wing, source_file)` is also present
  in the branch view is replaced by the branch row, and branch tombstones (`path_state:
  "deleted"`) hide the canonical row entirely. Overlay composition runs over the candidate
  window and widens the vector query (up to 10× the requested limit) so a low-scoring branch
  replacement shadows its canonical path without dropping unrelated results.

### `prune`

Purpose:
- Delete mined project/source data from the **local** palace by scope. Previews by default;
  deletes only with `--yes`. Never touches a remote palace, and never targets diary,
  narrative, or authored drawers — only the two project ingest kinds.

Flags:
- `--project-id <STRING>` (alias `--project`)
  The project as identified at mine time (explicit `--project-id` or the derived repo id).
- `--wing <STRING>`
  Wing to scope to. Taken from the project registry when `--project-id` is registered;
  required otherwise, and required when scoping by `--wing` + `--kind`.
- `--kind <projects|projects-branch>`
  Restrict to one ingest kind. Default: both project kinds.
- `--view <NAME>`
  Restrict to a single branch view; implies the `projects-branch` kind and excludes the
  canonical snapshot. Requires `--project-id`.
- `--source-prefix <PREFIX>`
  Restrict to source paths under this normalized prefix, matched against paths relative to
  the mined project root, e.g. `crates/legacy/`. Without `--view` this narrows the canonical
  snapshot only. Requires `--project-id`.
- `--dry-run`
  Preview only; never delete. This is already the behavior when `--yes` is absent.
- `--yes`
  Actually delete the matched sources.

Scope rules (prune refuses to run without a narrow scope):
- Pass `--project-id`, **or** both `--wing` and `--kind`. A bare `--wing` (or a bare `--kind`)
  is rejected, so the scope can never widen to every project or every kind.
- `--view` and `--source-prefix` both require `--project-id`, because the branch and path
  segments follow the project root key inside the source key.
- `--source-prefix` without `--view` skips branch views rather than over-matching (the branch
  segment precedes the path); the skip is reported as a `Note` line in the output.

Behavior:
- Prints the resolved scope (wing, kinds, project, view, path prefix, notes), the matched
  source count and drawer count, and up to 20 matched source keys followed by
  `… and N more`.
- Without `--yes`, ends with `Preview only — re-run with --yes to delete.` and exits `0`.
- With `--yes`, deletes each resolved source-key prefix across both stores and reports
  `Removed: N sources, M drawers`.
- Exit codes: `0` on success (including "nothing matched"); `2` for a scope that is too broad,
  invalid, or selects nothing; `1` when no palace exists at the resolved path.

Known limitation:
- Project data mined **before** the stable project-id migration is keyed by a checkout-path
  hash rather than `hash("project:<id>")`, so `--project-id` does not match those legacy
  rows. Re-mining migrates the **canonical** ones. Legacy `projects-branch` rows are never
  migrated or cleaned — ingest's legacy handling is canonical-only and the branch cleanup
  pass scans the stable prefix — so they persist and still appear in `view: "full"`
  searches. Sweep those with an explicit `--wing` + `--kind projects-branch` scope after
  confirming the preview. See
  [Mined Storage → Source keys](Mined-Storage.md#source-keys).

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

Each returns a non-zero result and prints a pointer to the Phase 9 deferral decision record
(`docs/rust-phase-plans/Phase09-Deferred-Commands.md`). That record is not published in this
repository — the deferral itself is recorded in [Release Scope](Release-Scope.md).

## Exit Behavior

- Successful command execution returns exit code `0`.
- Deferred-command and missing-palace flows return a non-zero result with explicit guidance.
- Clap parse failures still use Clap's normal non-zero error flow.
