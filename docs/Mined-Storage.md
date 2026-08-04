# Mined Storage — Locator Model

This page covers how project files are stored after mining, how snippet text is
recovered at read time, backwards compatibility, file discovery rules, and notes
relevant to federation.

## Storage model

When `mine` ingests a valid-UTF-8 project file, each chunk is stored as a
**locator row**: a `DrawerRecord` whose `content` field is persisted empty and
whose `locator` field records enough information to re-derive the chunk text from
the original file at read time.

Non-UTF-8 text files (e.g. Latin-1 encoded) that pass the binary sniff continue
to store chunk text verbatim with no locator.
Conversation exports, diary entries, and authored drawers always store content
verbatim, regardless of encoding.

### Locator fields

| Field | Type | Description |
|---|---|---|
| `byte_start` | `u64` | Byte offset of the first byte of the chunk in the original file (inclusive). |
| `byte_end` | `u64` | Byte offset one past the last byte of the chunk (exclusive). |
| `line_start` | `u32` | 1-based line number of the first line of the chunk. |
| `line_end` | `u32` | 1-based line number of the last line of the chunk. |
| `file_hash` | `String` | BLAKE3 hex digest of the full original file bytes at mine time. |
| `resolve_root` | `String` | Absolute checkout root on the palace host that owns the row. |
| `commit_hash` | `Option<String>` | Git commit SHA at mine time (`git rev-parse HEAD`); absent outside a git repo. |

`byte_start..byte_end` into the original file bytes is guaranteed to equal the
chunk text used for embedding and for computing the row's `content_hash`.

Files larger than 200,000 bytes are chunked from the first 200,000 bytes only.
Byte offsets are still relative to the full (untruncated) file, so `file_hash`
covers the complete original bytes and remains a valid integrity check even for
truncated files.

## Lazy resolution and stale semantics

Snippet text is never shipped in the stored row. It is resolved palace-side at
read time — during search, recall, wake-up, and any REST endpoint that returns
drawer content. There are three outcomes:

| Outcome | Condition | `stale` key in JSON |
|---|---|---|
| **Fresh** | File readable; `hash(current bytes) == file_hash`; byte slice decodes as UTF-8. | Absent |
| **Stale — best-effort text** | Hash mismatch; byte range still in-bounds and valid UTF-8 in the current file. | `"stale": true` |
| **Stale — placeholder** | File missing or unreadable, OR hash mismatch and range out-of-bounds or not valid UTF-8. | `"stale": true` |

Placeholder text takes the form `[stale] <source_file> missing; re-run mine`
or `[stale] <source_file> changed since mining; re-run mine`.

The `stale` JSON key is only present when `true`. Non-stale results are
byte-identical to pre-locator search output, so MCP clients and federation
receivers require no changes for the common case.

Resolution is **always palace-side**. Before any result crosses a federation
wire (or an MCP tool response), the owning palace resolves locators from its
own disk. `RemoteDrawerResult` carries an optional `stale` field (serde default;
wire-compatible in both directions) that is populated server-side.

## Backwards compatibility and migration

### Schema migration on open

LanceDB tables gain seven new nullable locator columns (`locator_byte_start`,
`locator_byte_end`, `locator_line_start`, `locator_line_end`,
`locator_file_hash`, `locator_resolve_root`, `locator_commit`) on first open
after upgrade. The migration uses `add_columns` with `CAST(NULL AS …)`
expressions — it is idempotent, requires no data rewrite, and completes without
downtime.

Tables also gain eight new nullable view-metadata columns (`view_repo_id`,
`view_name`, `view_source_path`, `view_head_commit`, `view_base_ref`,
`view_merge_base`, `view_worktree_id`, `view_path_state`) via the same
migration mechanism. These are populated on new project mines. Old rows read
back with `view_metadata: None` and continue to use the legacy `hall = "view:<name>"` convention for view scoping.

### View-metadata fields

| Field | Type | Description |
|---|---|---|
| `repo_id` | `String` | The project identity resolved at mine time — see below. |
| `view_name` | `Option<String>` | `None` for a canonical (default-branch) snapshot; the branch or detached-HEAD view name otherwise. |
| `source_path` | `String` | Absolute project checkout path on the palace host that owns the row. |
| `head_commit` | `Option<String>` | `git rev-parse HEAD` at mine time. |
| `base_ref` | `Option<String>` | Base/integration ref — the default branch name. |
| `merge_base` | `Option<String>` | Merge-base commit between the view and `base_ref`. |
| `worktree_id` | `String` | Stable worktree identity (hash of the canonicalized checkout path). |
| `path_state` | `String` | `"present"`, or `"deleted"` for a tombstone row whose source file was removed on the branch. |

`repo_id` is whatever project identity the mine resolved, stored verbatim — it is **not**
always a normalized remote URL:

| How the project was identified | `repo_id` value |
|---|---|
| Explicit `--project-id <id>` (or a registered project selected by one) | that string, exactly as given — e.g. `local/my-project` |
| Derived, repository root, `origin` present | normalized remote URL — `github.com/acme/repo` |
| Derived, project root is a repo subdirectory (monorepo) | `<normalized-origin>#<project-root>` — e.g. `github.com/acme/repo#services/api` |
| Derived, no usable `origin` | `wing:<wing>` |

Anything correlating view metadata back to a project must accept all four shapes; assuming
a bare remote URL computes the wrong identity for explicit-ID and monorepo projects.

### Old rows keep working

Rows written before the locator upgrade read back with `locator: None` and their
stored `content` intact. No data is lost and no manual step is required.

Rows written before the view-metadata upgrade read back with
`view_metadata: None`. View-scoped search filters (`--view <name>` or
`view: "branch_name"` in the API) still match legacy rows via a
`view_name IS NULL AND hall = 'view:<name>'` fallback in the SQL filter, so
existing branch-delta mines remain searchable without re-mining.

### Converting old rows to locator rows

Run `mine --reindex <dir>` to re-mine a directory and replace content rows with
locator rows:

```bash
mempalace-cli mine /path/to/project --reindex
```

`--reindex` bypasses the unchanged-content skip that would normally leave
already-ingested files alone. After re-mining, chunks for valid-UTF-8 files
store empty content and a full locator.

### Populating view-metadata on legacy branch rows

Run `mine --branch --reindex <dir>` to re-mine a branch delta and populate the
`view_*` columns on the re-ingested drawers. The `hall = "view:<name>"` field
remains set on legacy rows that are not re-mined; the new columns are preferred
when present.

### Non-UTF-8 fallback

Files that are valid according to file discovery rules but whose bytes are not
valid UTF-8 always use the legacy stored-content path. Re-mining them with
`--reindex` does not convert them to locator rows.

## Discovery rules

`mine --mode projects` discovers eligible sources in one of two ways:

- **Git-backed roots** enumerate the tracked index (`git ls-files`): only
  tracked index files are mined (this includes staged, uncommitted entries,
  but never untracked or ignored working-tree content). Ignored and untracked
  working-tree content (`.gitignore`d files such as `.env`, local editor
  overrides like `*.local.json`, and build output) never enters the source set
  because it is simply absent from the index. A `.gitignore` does **not**
  suppress a tracked file: tracked paths remain tracked even after an ignore
  pattern is added. `.mempalaceignore` is the explicit additional exclusion and
  applies to tracked files, including nested files at any depth. Independently
  of git, the secret-path denylist still applies to tracked index paths: a
  secret-shaped file that was committed (e.g. a tracked `.env`) is withheld and
  reported, never mined. Branch-delta
  mines (`--branch` / `--view <name>`) are the deliberate exception: they also
  include untracked, non-ignored files so that new branch work is captured
  before it is committed.
- **Non-Git directories** use a filesystem walk that honors `.gitignore` and
  `.mempalaceignore` files at every directory level with git-compatible
  semantics: nested files are scoped to their own directory, `!` patterns
  re-include previously excluded paths, patterns containing a `/` are anchored
  to the ignore file's directory (unanchored patterns match the basename at any
  depth), and `*`, `?`, `[...]`, and `**` globs follow git's rules. Git-only
  details are preserved exactly: a leading `\#` or `\!` escapes a literal
  hash/bang that would otherwise open a comment or a negation, a trailing `/**`
  matches everything *inside* the named directory but not the directory itself
  (so `abc/**` + `!abc/keep.md` keeps `abc/keep.md`, as in git), and a trailing
  space is ignored unless it is escaped (`foo\ ` targets a filename literally
  ending in a space). The global excludes file (`core.excludesFile`, defaulting
  to `$XDG_CONFIG_HOME/git/ignore`, or `~/.config/git/ignore`) also applies,
  since it is user-level git configuration rather than a repository concept.

The repository-level exclude sources are honored at git's precedence:
`$GIT_DIR/info/exclude` (Git-backed roots only) and the global excludes file
(`core.excludesFile`) for every filesystem walk, including non-Git roots. As in
git, per-directory `.gitignore` patterns outrank `info/exclude`, which outranks
the global excludes file; these two repository-level sources are purely
additive and never override the `.mempalaceignore` local protection.
Repository-level excludes never apply to tracked index files: a tracked path
stays eligible even when an exclude file names it.

The following directory names are always skipped: `.git`, `node_modules`,
`__pycache__`, `.venv`, `venv`, `env`, `dist`, `build`, `.next`, `coverage`,
`.mempalace`.

Linked Git worktrees reported by `git worktree list --porcelain` are always
skipped, preventing duplicate checkout content from being mined.

Room detection during `init` and `project register` uses the same safe source
set: rooms are derived from the directories that hold eligible sources, so
ignored, untracked, and linked-worktree directories never produce rooms.

### Accepted extensions

The following extensions are accepted (case-insensitive match on the normalized
lowercase extension with leading dot):

| Category | Extensions |
|---|---|
| Text / markup | `.txt` `.md` `.html` `.xml` `.csv` `.json` `.yaml` `.yml` `.toml` `.ini` `.cfg` `.conf` `.properties` |
| Web / frontend | `.js` `.ts` `.jsx` `.tsx` `.css` `.vue` `.svelte` `.astro` |
| Systems languages | `.rs` `.c` `.h` `.cc` `.cpp` `.cxx` `.hh` `.hpp` `.m` `.mm` `.zig` `.nim` |
| JVM / Android | `.java` `.kt` `.kts` `.scala` `.sbt` `.groovy` `.gradle` |
| .NET | `.cs` `.fs` `.fsi` `.fsx` |
| Scripting / dynamic | `.py` `.rb` `.php` `.lua` `.pl` `.pm` `.r` `.jl` `.dart` |
| Shell | `.sh` `.bash` `.zsh` `.fish` `.ps1` `.psm1` `.psd1` `.bat` `.cmd` |
| Functional / BEAM | `.ex` `.exs` `.erl` `.hrl` `.clj` `.cljc` `.cljs` `.edn` |
| Mobile / other | `.swift` `.go` |
| SQL / data | `.sql` |
| IaC / config | `.tf` `.tfvars` `.hcl` `.proto` `.graphql` `.gql` `.dockerfile` |

### Extensionless basenames

The following extensionless file names are always accepted: `Dockerfile`,
`Makefile`, `Rakefile`, `Gemfile`, `Jenkinsfile`, `Vagrantfile`.

### Shebang detection

An extensionless file not in the basename allowlist is accepted if its first 256
bytes begin with `#!`.

### Binary sniff

Every candidate file (including extension- and basename-matched files) is
rejected if any of the first 8 KiB of bytes is a NUL byte (`0x00`). This
excludes misnamed binaries, compiled outputs, and other non-text data.

### Always-skipped files

The following files are never discovered regardless of extension:

- **Secrets** (the path-based secret denylist, matched case-insensitively on
  the file name **before any content is read**, in both Git-index and
  filesystem discovery):
  - `.env` / `.env.*` — process-environment files (any name starting with
    `.env`) and `*.env` (any name ending in `.env`)
  - `*.kubeconfig*` — Kubernetes configuration files
  - `id_rsa*`, `id_ed25519`, `id_ecdsa`, `id_dsa` — SSH private keys
  - `*.pfx`, `*.p12`, `*.jks` — keystores and truststores
  - `.npmrc`, `.netrc` — package/registry credential files
  - `*.tfstate`, `*.tfvars` — Terraform state and variable files
  - `secrets*.json` — JSON secret bundles
  - `*.local.json` — local override/config files that commonly hold credentials
- **Lockfiles**: `package-lock.json`, `Cargo.lock`, `yarn.lock`,
  `pnpm-lock.yaml`, `poetry.lock`, `composer.lock`, `Gemfile.lock`
- **Palace config**: `mempalace.yaml`, `mempalace.yml`, `mempal.yaml`,
  `mempal.yml`, `.gitignore`

Every secret-denylist exclusion is counted in the run's discovery metrics (the
`Files ignored` line / `ProjectSourceDiscovery.skipped`) exactly like any other
skipped candidate, and is also emitted as an **operator-visible skip record**
with the withheld path and a short reason — but never any file content. The
mine summary reports these as `Secrets withheld: N` followed by one
`<path> — secret-shaped path (<reason>)` line per path, so an operator can see
what was withheld rather than having to audit after the fact.

## Federation notes

- **Resolution is palace-side.** The palace that owns a mined row resolves the
  snippet from its local checkout before returning it to any client or remote
  peer. Clients and federated palaces receive plain text; they do not need access
  to the source checkout.
- **`resolve_root` is re-rootable.** Byte/line ranges and `source_file` are
  relative to the checkout root; the absolute path is stored separately. The
  bulk-ingest endpoint lets the receiving palace fill in its own checkout path
  via `server.checkouts`.
- **`content_hash` is the dedupe key.** The row's `content_hash` is computed
  from the real chunk text even though that text is not persisted.
  Branch-delta mining identifies identical chunks across branches by this hash.
- **`commit_hash` anchors invalidation.** Recording the git SHA at mine time
  provides the reference point for merge-base invalidation of branch deltas.

## Federated mining

When a **canonical** mine's wing routes to `mode: remote` (or `mode: combined`
with `write: remote`), running `mine <dir>` routes to the remote palace instead
of writing locally. When the route resolves to `mode: combined` with
`write: both`, the mine runs locally first, then a best-effort remote push is
attempted (see [Federation guide](Federation.md#write-both--local-first-dual-write-semantics)).

A mine that resolves to a **branch view** — via `--branch`, `--view <name>`, or
automatic detection on a non-canonical checkout — never routes remote. It always
writes to the local palace, whatever the wing's route says.

### Flow

1. The CLI runs full project discovery, chunking, byte/line-offset calculation,
   and room detection locally — the same pipeline as a local mine — but skips
   embedding and storage (`prepare_project_batch`).
2. It calls `GET /v1/info` on the remote server and requires `"ingest"` in the
   capabilities list. If the capability is absent the run fails with a clear
   message asking the operator to upgrade the remote server (old servers return
   404 for the new route, which produces a `RemoteRejected` error).
3. The prepared files are split into request batches (see [Batching](#batching)
   below) and sent to `POST /v1/ingest/batch`.
4. The server embeds each chunk with its own embedding model and commits the
   drawers using the same pending-run / replace-source machinery as a local mine.
5. Per-file results (`ingested`, `skipped_unchanged`, `failed`) are aggregated
   and printed in the same mine-summary format as a local run.

If the remote is unreachable and the route is `remote` or `combined` with
`write: remote`, the run fails with an explicit error. There is no silent
fallback to local storage (matching the write semantics of other federated
operations). For `write: both`, the local mine still succeeds and the remote
failure is appended to the mine output without rolling back.

If the current git branch differs from the repository's default branch, `mine`
prints a warning line before sending. The run is not blocked.

Dry-run mode (`--dry-run`) prepares the batch and prints the plan without
sending any network requests.

### Wire format

Request body for `POST /v1/ingest/batch`:

```
IngestBatchRequest {
    wing:        String,           // target wing name
    repo_id:     String,           // machine-independent repo identity
    agent:       Option<String>,   // client-declared agent name
    commit_hash: Option<String>,   // git SHA at mine time
    files:       Vec<IngestFileDto>,
}
```

Each `IngestFileDto`:

```
IngestFileDto {
    relative_path: String,         // project-root-relative, forward slashes
                                   // (= repo-root-relative when mining a whole
                                   //  repo; server.checkouts must use that root)
    content_hash:  String,         // skip-unchanged key
    file_hash:     Option<String>, // BLAKE3 of full file bytes; None => content rows
    chunks:        Vec<IngestChunkDto>,
}
```

Each `IngestChunkDto`:

```
IngestChunkDto {
    chunk_index: u32,
    room:        String,
    text:        String,           // server embeds this
    // present together iff file_hash is Some:
    byte_start:  Option<u64>,
    byte_end:    Option<u64>,
    line_start:  Option<u32>,
    line_end:    Option<u32>,
}
```

Response body `IngestBatchResponse`:

```
IngestBatchResponse {
    files:    Vec<IngestFileResult>,
    warnings: Vec<String>,         // non-fatal; e.g. missing checkout mapping
}
```

Each `IngestFileResult`:

```
IngestFileResult {
    relative_path:   String,
    status:          String,   // "ingested" | "skipped_unchanged" | "failed"
    drawers_written: usize,    // 0 for skipped or failed
    error:           Option<String>,
}
```

### Repo identity and source keys

`repo_id` is derived from the git remote URL of `origin` and normalized to a
machine-independent form by `derive_repo_id` / `normalize_git_remote_url`:

- Trailing `.git` and trailing `/` are stripped.
- SCP-style `git@host:path` becomes `host/path`.
- URL-style `scheme://[user@]host[:port]/path` becomes `host/path` (port
  dropped; host lowercased; path case preserved).

Examples from the test suite:

| Remote URL | Normalized `repo_id` |
|---|---|
| `git@github.com:Acme/Repo.git` | `github.com/Acme/Repo` |
| `https://github.com/acme/repo.git/` | `github.com/acme/repo` |
| `ssh://git@Host.Example:2222/team/repo` | `host.example/team/repo` |
| `https://user@gitlab.com/a/b` | `gitlab.com/a/b` |

When no `origin` remote is configured, `repo_id` falls back to
`wing:<wing_name>`.

The server computes the source key for each file as:

```
projects:{wing}:{blake3_hex(repo_id)}:{relative_path}
```

This is the same shape as the local canonical key
(`projects:{wing}:{blake3_hex("project:" + repo_id)}:{relative_path}`, see
[Source keys](#source-keys)); the hash input differs because the two keyspaces
live in separate palaces and are never compared directly. What matters is that
both are derived from repository identity rather than a machine-local checkout
path: two clients pushing the same repository to the same remote wing converge on
identical source keys and identical drawer ids.

Federated batches are **always canonical**. The server stamps
`view_name: None` on every row it ingests through this endpoint, and the batch
DTOs carry no view metadata — branch views stay in the client's local palace.

Drawer ids follow the same formula as local mining:

```
{wing}/{room}/{first-12-of-blake3_hex(source_key)}-{chunk_index:04}
```

### Hub-side stale semantics

The receiving server stores **locator rows** (not chunk text) for files where
`file_hash` is present. The `resolve_root` field of every locator row is filled
from `server.checkouts[wing]` (see [Config-Schema.md](Config-Schema.md)).

- **Checkout mapped:** locators resolve fresh text from the server's local
  checkout.
- **Checkout not mapped:** `resolve_root` is stored as an empty string. Every
  search result for those rows resolves as a stale placeholder until
  `server.checkouts` is configured. The response `warnings` array contains:
  `"no checkout configured for wing '<w>'; locator results will resolve as stale placeholders until server.checkouts is set"`.

Files whose `file_hash` is `None` (non-UTF-8 fallback) are stored as legacy
content rows — chunk text is persisted verbatim with no locator.

### Skip and replace semantics

Processing is per-file:

- If `get_ingested_file(source_key)` returns a record whose `content_hash`
  matches the request file's `content_hash`, the file is reported
  `skipped_unchanged` (no embedding, no storage write).
- Otherwise the server embeds all chunks, then calls `replace_source_drawers`,
  which commits the new drawers and deletes any stale drawer ids for the same
  source key (handles files that shrank between mines).

One `mine_batch` change event is appended per request when at least one file
was ingested. The event carries `repo_id`, `files_ingested`, `files_skipped`,
`files_failed`, `drawers_written`, and `commit_hash` in its details.

### Batching

To stay within the server's body limit, the client flushes a new request batch
when either limit is reached:

| Limit | Value |
|---|---|
| Files per batch | 64 |
| Chunk text per batch | ~4 MiB (4 × 1 024 × 1 024 bytes) |

The server applies a 16 MiB body size limit to `POST /v1/ingest/batch` (axum's
default is 2 MiB; all other routes keep the default).

### `relative_path` sanitization

The server validates `relative_path` on every file before processing:

- Must not be empty.
- Must not start with `/` (absolute path).
- Must not contain `\` (backslash).
- Must not contain `:` (Windows drive letter or scheme).
- Must not contain `..` path segments (directory traversal).
- Must not contain a `.git` path segment (prevents reading `.git/config` or
  credentials back through locator resolution).

Files that fail validation are reported as `"failed"` with a descriptive error;
the rest of the batch continues processing.

### Diary guard

The server rejects any request targeting the diary wing (`wing_agents`) or any
chunk whose room is the diary room (`diary`) with HTTP 422 and error code
`diary_not_federated`. Diary entries are always palace-local.

## Repository views

Every project mine writes into exactly one **view** of the repository:

- The **canonical** view is the default-branch snapshot. It uses the `projects`
  ingest kind and carries `view_name: None`.
- A **branch** view is a delta against the canonical snapshot. It uses the
  `projects-branch` ingest kind and carries `view_name: Some(<name>)`.

### Automatic view detection

`mine --mode projects` classifies the checkout before ingesting, via
`detect_checkout_view`:

| Checkout | Detected view | Result |
|---|---|---|
| HEAD is on the repository's default branch | `Canonical` | full canonical mine |
| HEAD is on any other branch | `Branch { view_name: <branch> }` | branch-delta mine |
| Detached HEAD | `Branch { view_name: "detached-<12-hex>" }` | branch-delta mine; the hex is a hash of the repository toplevel path |
| Not a Git repository, or no resolvable default branch | `NonGit` / `Canonical` | full mine (pre-view behaviour preserved) |

The default branch is resolved by trying `git symbolic-ref --short
refs/remotes/origin/HEAD`, then the literal `main`, then `master` — the first
reference `git rev-parse --verify` accepts wins.

> **No resolvable default branch means every checkout looks canonical.** A repository
> whose integration branch is named something else (`trunk`, `develop`) and which has no
> `origin` falls into the `(None, _)` arm, which returns `Canonical` deliberately: without
> an integration ref there is no safe delta baseline, so the pre-view behaviour of mining
> the checkout in full is preserved. The consequence is that a plain `mine` on a feature
> branch of such a repository **overwrites the canonical snapshot** with that branch's
> contents rather than storing a view. Pass `--branch` or `--view <name>` explicitly on
> those repositories.

Explicit selectors override detection: `--full` and `--view canonical` force a
canonical mine; `--branch` and `--view <name>` force a branch-delta mine.
`--full` conflicts with `--view`, and `--branch` conflicts with `--view canonical`.

An **automatically** detected branch mine additionally requires that a canonical
snapshot already exists for the project. Without one the run fails rather than
silently storing a delta with nothing to overlay; pass `--full` to mine the whole
checkout instead. Explicit `--branch` / `--view` bypasses this guard.

### Source keys

Canonical rows:

```
projects:{wing}:{blake3_hex("project:" + repo_id)}:{relative_path}
```

Branch rows add the view name as its own segment, so branch views neither collide
with the canonical snapshot nor with each other:

```
projects-branch:{wing}:{blake3_hex("project:" + repo_id)}:{view_name}:{relative_path}
```

The root key is derived from the **stable project identity** (`repo_id` — the
normalized git origin, an explicit `--project-id`, or `wing:<wing>`), not from the
local checkout path. Two checkouts of the same repository on the same machine
therefore converge on the same keys.

> **Legacy keys, and the limits of the migration.** Rows mined before the stable
> project-id migration used `blake3_hex(<absolute checkout path>)` as the root key.
>
> **Canonical legacy rows migrate on the next mine.** Ingest computes the legacy key
> alongside the stable one, removes the old row when it replaces it, and sweeps the
> remaining legacy prefix for paths no longer present.
>
> **Branch legacy rows do not.** That whole path is gated on `branch_name.is_none()`, and
> the branch cleanup pass scans only the stable-project prefix, so legacy
> `projects-branch` rows are neither migrated nor removed by re-mining. They stay in the
> palace and can still surface in `view: "full"` searches. Remove them explicitly with a
> `--wing` + `--kind projects-branch` prune after checking the preview —
> `prune --project-id` cannot select them, because it builds the stable-identity prefix
> these rows were never keyed under. See [CLI Surface → `prune`](CLI-Surface.md#prune).

### Overlay composition at search time

Search is view-scoped rather than view-blind:

- No `view` (or `view: "canonical"`) — canonical rows only; branch views are
  excluded.
- `view: "<branch>"` — the branch view is composed **over** the canonical
  snapshot. For each `(wing, source_file)` present in the branch view, the branch
  row replaces the canonical row. A branch row with `path_state: "deleted"` (a
  tombstone) removes the path from the result set entirely.
- `view: "full"` — every stored repository view is searched independently, with no
  composition.

`(wing, source_file)` is the composition key rather than `repo_id`, so mixed-version
replicas that do not share durable repository IDs still compose correctly.

Because a branch replacement can score lower than the canonical row it shadows,
search widens the candidate window (doubling up to 10× the requested limit) and
re-composes until a full result page survives filtering. Overlay lookups are bounded
to the source files present in the current candidate window, so composing a view never
loads the whole branch.

### Tombstones

On a branch-delta mine, files deleted on the branch relative to the merge-base are
recorded as tombstone rows: a drawer under the branch source key with
`path_state: "deleted"` and the fixed content `Deleted branch path tombstone`. A
tombstone hides its path in **every** room of the view, not just the room it is
stored in.

Tombstones are durable and idempotent — an unchanged tombstone is left in place
across runs and is not counted in `Sources removed`.

## Branch-delta mining

`mine --branch` mines only the files that differ from the repository's default
branch, making it efficient for ongoing branch work. Automatic detection produces
the same result on a non-canonical checkout without the flag.

### Delta computation

1. The default branch reference is resolved by trying, in order:
   - `git symbolic-ref --short refs/remotes/origin/HEAD`
   - literal `main`
   - literal `master`
   The first reference that `git rev-parse --verify` accepts is used.
   If none resolves, `mine --branch` exits with an error.

2. The merge-base commit between the default branch reference and `HEAD` is
   computed with `git merge-base`.

3. The delta set is the union of:
   - Files changed or added in the working tree relative to the merge-base
     (`git diff --name-only --diff-filter=d <merge-base>`).
   - Untracked files (`git ls-files --others --exclude-standard`).

   Uncommitted edits are intentionally included — the purpose is to embed
   exactly what you are working on, not just what is committed.

4. Paths reported by git are repo-root-relative. When the project root is a
   subdirectory of the repo, paths are re-relativized to the project root;
   paths outside the project root are dropped.

### Source-key namespace

Branch runs use the `projects-branch` namespace and carry the view name as a key
segment — see [Source keys](#source-keys) above. This ensures branch rows never
collide with a canonical mine of the same wing, nor with another branch view,
allowing all of them to coexist in one palace and be composed at search time.

### Cleanup pass

On every branch-delta run, after ingesting the delta, the CLI lists all source
keys under `projects-branch:{wing}:{root_key}:{view_name}:` and replaces drawers
for any file whose key is **not** in the current delta and is **not** a live
tombstone (file reverted to base). The replacement is an empty commit — it
removes stale drawers without leaving orphaned rows. The count of removed sources
is reported as `Sources removed: N` in the mine summary.

Files deleted on the branch are not simply dropped: they are replaced by
tombstone rows (see [Tombstones](#tombstones)) so they keep shadowing the
canonical snapshot. An unchanged tombstone is left in place and does not count
towards `Sources removed`.

This means the branch store is always consistent with the current delta: rebase,
merge, or reverting a file immediately cleans up the previously mined drawers on
the next run.

### Branch-delta is always local

A resolved branch view overrides the wing's federation route — whether it came
from `--branch`, `--view <name>`, or automatic detection. Even if the wing is
configured as `mode: remote` or `mode: combined` with `write: remote`, a branch
mine always writes to the local palace; only canonical mines are eligible for
federated batch ingest. The intended workflow is:

- Team or CI mines the full repository into the **remote** shared palace via
  the normal `mine` command (remote route).
- Each developer mines their local branch delta into their **local** palace.
- When search runs over a combined wing, both sides are merged. The
  `content_hash` deduplication ensures chunks that appear in both the remote
  full mine and the local branch delta are not double-counted.

`--branch` is not supported for conversation (`--mode convos`) mining; using
both together exits with an error.
