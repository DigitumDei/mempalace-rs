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

Behavior:
- `projects` uses the project ingest path.
- `convos` uses the conversation ingest path.
- In low-CPU mode, ingest batching is clamped by the resolved low-CPU runtime config.

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

### Deferred Commands

These commands are intentionally visible but not shipped as working Rust v1 functionality:

- `split`
- `compress`

Each returns a non-zero result and points at [Phase09-Deferred-Commands](../rust-phase-plans/Phase09-Deferred-Commands.md).

## Exit Behavior

- Successful command execution returns exit code `0`.
- Deferred-command and missing-palace flows return a non-zero result with explicit guidance.
- Clap parse failures still use Clap's normal non-zero error flow.
