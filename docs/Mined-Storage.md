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

### Old rows keep working

Rows written before the locator upgrade read back with `locator: None` and their
stored `content` intact. No data is lost and no manual step is required.

### Converting old rows to locator rows

Run `mine --reindex <dir>` to re-mine a directory and replace content rows with
locator rows:

```bash
mempalace-cli mine /path/to/project --reindex
```

`--reindex` bypasses the unchanged-content skip that would normally leave
already-ingested files alone. After re-mining, chunks for valid-UTF-8 files
store empty content and a full locator.

### Non-UTF-8 fallback

Files that are valid according to file discovery rules but whose bytes are not
valid UTF-8 always use the legacy stored-content path. Re-mining them with
`--reindex` does not convert them to locator rows.

## Discovery rules

`mine --mode projects` uses the following acceptance rules on every file found
under the target directory. `.gitignore` and `.mempalaceignore` files are
honored throughout, and the following directory names are always skipped:
`.git`, `node_modules`, `__pycache__`, `.venv`, `venv`, `env`, `dist`, `build`,
`.next`, `coverage`, `.mempalace`.

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

- **Secrets**: `.env`, `.env.*` (any name starting with `.env`)
- **Lockfiles**: `package-lock.json`, `Cargo.lock`, `yarn.lock`,
  `pnpm-lock.yaml`, `poetry.lock`, `composer.lock`, `Gemfile.lock`
- **Palace config**: `mempalace.yaml`, `mempalace.yml`, `mempal.yaml`,
  `mempal.yml`, `.gitignore`

## Federation notes

- **Resolution is palace-side.** The palace that owns a mined row resolves the
  snippet from its local checkout before returning it to any client or remote
  peer. Clients and federated palaces receive plain text; they do not need access
  to the source checkout.
- **`resolve_root` is re-rootable.** Byte/line ranges and `source_file` are
  relative to the checkout root; the absolute path is stored separately. A future
  bulk-ingest endpoint (#20) can ship locators without `resolve_root` and let the
  receiving palace fill in its own checkout path.
- **`content_hash` is the dedupe key.** The row's `content_hash` is computed
  from the real chunk text even though that text is not persisted.
  Branch-overlay mining (#20) identifies identical chunks across branches by
  this hash.
- **`commit_hash` anchors future invalidation.** Recording the git SHA at mine
  time provides the reference point for merge-base invalidation of branch deltas
  in #20.
