# Upgrade from MemPalace to AgentPalace

AgentPalace now uses `agentpalace` (`agentpalace.exe` on Windows), `agentpalace_*`
MCP tools, `AGENTPALACE_*` settings, `agentpalace-*` crates and release assets,
`agentpalace.yaml`, and `~/.agentpalace`. The bundled coordination skill is
`coordinate-with-agentpalace`; hooks use `agentpalace-*-hook.sh` names.

This is a coordinated cutover. Stop palace services and close MCP hosts that can
restart them, upgrade the server and clients together, then restart the hosts so
they discover the new tool list. Old MCP tool names are not aliases. REST routes
and persisted data schemas are unchanged. Memories, project IDs, wing IDs,
collection names, receipts, and coordination records keep their original values.

## Installer upgrade

Use the normal [installer](Quickstart.md). After verifying the signed download,
the installer runs the new executable's migration command **before** replacing
any executable. Migration also runs with `--no-setup` / `-NoSetup`; that flag skips
new registrations and model warm-up, not migration of existing registrations.

Migration:

1. Refuses to proceed while a MemPalace or AgentPalace process is running. Service
   managers and MCP hosts must remain stopped throughout the upgrade.
2. Validates existing supported client configuration before copying data.
3. Copies `~/.mempalace` into a staging directory beside `~/.agentpalace`, then
   publishes the complete copy by renaming the directory. The old `bin` directory
   is retained only in the backup; the installer supplies the new executable.
4. Retains the original home as `~/.mempalace.pre-agentpalace`. The default model
   cache under the OS cache directory migrates from `mempalace` to `agentpalace`
   the same way, avoiding an unnecessary model download. External `HF_HOME`
   caches are unchanged. If the process's `HF_HOME` points inside a home being
   moved, migration stops and reports its replacement path; update that setting
   before retrying so subsequent processes cannot recreate the retired home.
5. Updates `config.json` paths that point inside the migrated home. Custom palace
   and token paths outside that home remain where they are. Project registry
   entries and stored memory contents are not rewritten.
6. Renames existing `mempalace` MCP registrations to `agentpalace`, points local
   launchers at the new executable, preserves custom arguments and credentials,
   and updates known environment names and tool allow/deny lists. Remote URL
   registrations retain their URL. Original client documents are retained beside
   each file with a `.pre-agentpalace` suffix.

Supported existing registrations are Claude Code (`~/.claude.json`), Codex
(`$CODEX_HOME/config.toml`, otherwise `~/.codex/config.toml`), Gemini CLI,
Copilot CLI, Antigravity's two supported config locations, and strict-JSON
OpenCode configuration (respecting `XDG_CONFIG_HOME`). Active tool references in
`identity.txt`, Codex's global `AGENTS.md`, and `~/.claude/CLAUDE.md` are updated;
durable wing IDs and historical memories are preserved.

The installer adds the new executable directory to PATH. Windows also removes
the retired default directory from user PATH. On Unix the new entry takes
precedence; an existing shell line referencing the retired directory can be
removed after verification. Restart terminals to pick up PATH changes.

The installer passes the selected data home to setup, which records it in new
MCP registrations. Existing explicit client configuration homes outside the
migrated directory are preserved.

## Custom installations and deliberate exceptions

Unix installers accept `--migrate-from <old-home>` and `--migrate-to <new-home>`.
PowerShell accepts `-MigrationFrom <old-home>` and `-MigrationTo <new-home>`.
Both accept their existing install-directory override for the executable.
The default executable directory is `<new-home>/bin`. It must be outside the old
home and its backup. A custom data/config home is never guessed or recursively
searched: pass these options to move it, or retain it and set
`AGENTPALACE_CONFIG_DIR` explicitly.

For a source-built executable or a preview:

```sh
agentpalace migrate --mcp-path /absolute/install/path/agentpalace --dry-run
agentpalace migrate --mcp-path /absolute/install/path/agentpalace
```

The preview does not write files or inspect running processes. The applying
command requires all servers to be stopped. It works without downloading models.
The executable path describes the **final installed path**, not a temporary
download. Install that executable before restarting any clients.

Existing `mempalace.yaml` and the older `mempal.yaml` remain readable; new files
use `agentpalace.yaml`. Rename checked-in project config files in their own
repositories when convenient; do not rename the wing or project ID inside them.
Legacy `MEMPALACE_*` process settings remain accepted as upgrade aliases, with
`AGENTPALACE_*` taking precedence. Update shell profiles and service environments
to the new names. Custom old configuration paths that were moved must be updated
to their new destination.

Manually update custom wrapper scripts, service units, project-scoped MCP
registrations, JSONC files, nonstandard MCP registration names, hook paths, and
project-specific agent instructions. Migration refuses a recognized old MCP
entry using an unknown local launcher instead of overwriting it. A client with
both MCP names configured is a conflict and must be resolved explicitly.
Registered skill records are historical data: publish updated skill versions
with the new instructions reference and required tool names as needed.

## Recovery and rollback

If both data homes already exist, migration refuses to merge them. Inspect them
and choose a destination explicitly. It also refuses symlinks in the copied tree
and refuses to overwrite a previous source backup. Custom external storage can
remain outside the home, referenced by its configured path.

A marker in the new home permits a repeated run to finish retiring the original
after interruption between publication and backup. Interrupted staging copies
are never treated as live data. Completed client updates are idempotent; rerun
the installer after resolving a reported error. Migration failures are fatal to
installation, while model warm-up failures retain the installer's existing
warning and remediation behavior.

For rollback, stop all servers/hosts, retain the new home and any new writes,
restore the old home from `.pre-agentpalace`, and restore the backed-up client
documents and old executable/PATH. Do not overwrite either palace with the
other: a pre-upgrade backup does not contain writes made after upgrading. Verify
`agentpalace status`, a known search, credentials, and client connections before
manually removing any backups.

## Release operators

Publish a signed candidate and stable release containing the **new** asset names
before directing users to the updated installer. The updated installer does not
install an old-name executable from an older stable release. Existing release
signatures and the embedded verification key are unchanged. During the cutover,
workflows prefer `AGENTPALACE_RELEASE_SIGNING_KEY` and
`AGENTPALACE_IMMUTABLE_RELEASES_ENABLED`, with the old repository secret/variable
names retained as fallbacks so publishing is not interrupted. The repository
configuration helper writes the new names.
