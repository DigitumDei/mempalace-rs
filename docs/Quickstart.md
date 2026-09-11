# Quickstart

Get AgentPalace running and connected to your AI in a few minutes.

Upgrading from `mempalace-rs`? Stop existing palace servers and MCP hosts first.
The installer migrates data and supported registrations with backups; see
[Migration](Migration.md) for custom paths and rollback.

## 1. Install

The installer downloads the latest stable binaries for your platform, verifies the signed release manifest and the artifact digests, installs them to `~/.agentpalace/bin`, adds that directory to your PATH, and runs `agentpalace setup` to register the MCP server with detected AI tools and warm the embedding model.

**macOS (Apple Silicon) / Linux (x86_64, glibc 2.38+):**

```bash
curl -fsSL https://raw.githubusercontent.com/DigitumDei/agentpalace/main/install.sh | sh
```

**Windows (x86_64, PowerShell 7.1+):**

```powershell
irm https://raw.githubusercontent.com/DigitumDei/agentpalace/main/install.ps1 | iex
```

Options:

| sh flag | ps1 parameter | Effect |
|---|---|---|
| `--no-setup` | `-NoSetup` (or `$env:AGENTPALACE_NO_SETUP='1'`) | skip MCP registration and model warm-up |
| `--no-path` | `-NoPath` (or `$env:AGENTPALACE_NO_PATH='1'`) | don't touch your PATH |
| `--install-dir <dir>` | `-InstallDir <dir>` (or `$env:AGENTPALACE_INSTALL_DIR`) | install elsewhere |
| `--channel nightly --version v<version>-nightly.<full-commit-sha>` | `-Channel nightly -Version v<version>-nightly.<full-commit-sha>` | explicitly install an immutable test candidate |

Pass sh flags through the pipe with `| sh -s -- --no-setup`. For PowerShell parameters, download the script first (`irm ... -OutFile install.ps1`) — env vars work with the piped one-liner.

Stable releases are immutable and are the default installer target. Candidates from `main` are immutable `v<version>-nightly.<full-commit-sha>` prereleases and require the explicit nightly channel and version above.
The installer never falls back to the legacy mutable `nightly` tag. Before the first stable release exists, it exits without changing an existing installation.

The Windows installer verifies release signatures with cryptography APIs introduced in PowerShell 7.1. Install PowerShell 7.1 or later before using the Windows command above.

Supported platforms: Linux x86_64 (glibc 2.38+), macOS Apple Silicon, Windows x86_64. Anything else (Intel macOS, ARM Linux, musl) has no prebuilt ONNX Runtime — build from source instead.

### 1b. Build from source (alternative)

```bash
cargo build --release -p agentpalace-cli
```

Requires Rust 1.88+ and `protobuf-compiler` (see [README](../README.md)).

Expected binaries:
- `target/release/agentpalace`

ONNX Runtime is statically linked into the executable. Model weights are cached separately.

The rest of this guide assumes `agentpalace` is on your PATH (the installer does this); for a source build, substitute `./target/release/agentpalace`.

## 2. Initialize a palace

Point `init` at any project directory. It detects folders and stores the wing
and room configuration in the local registry (`~/.agentpalace/projects.json` by
default), so no repository file is required:

```bash
agentpalace init /path/to/your/project
```

Use `--repo-config` when you explicitly want a portable repository-local
`agentpalace.yaml` as well.

For a repository without a Git `origin`, provide a durable identity explicitly
so another checkout can resolve the same declaration:

```bash
agentpalace init /path/to/your/project --project-id local/my-project
```

On first run, you'll see a startup validation status. If it's not `ready`, set `AGENTPALACE_EMBED_ALLOW_DOWNLOADS=1` to download embedding assets, then re-run `init`.

## 3. Ingest data

```bash
# Mine project files (code, docs, notes)
agentpalace mine /path/to/your/project

# Mine conversation exports
agentpalace mine /path/to/chats/ --mode convos --wing project_name
```

`mine` reads the checkout it is pointed at. On the repository's default branch it takes a
full **canonical** snapshot; on any other branch it mines only the delta against that
branch as a named **view**, which then composes over the canonical snapshot at search time.
So mine the default branch first, then just re-run `mine` as you work on a feature branch.
`--full` forces a canonical mine, `--branch` forces a delta — see
[Mined Storage](Mined-Storage.md#repository-views).

> **If your integration branch isn't `main` or `master`, pass `--branch` explicitly.**
> Detection resolves the default branch from `origin/HEAD`, then literal `main`, then
> `master`. When none of those resolve — a local repo whose integration branch is `trunk`,
> say, with no `origin` — there is no safe delta baseline, so every checkout is treated as
> canonical. Plain `mine` on a feature branch would then overwrite the canonical snapshot
> with that branch's contents instead of storing a view.

## 4. Verify it works

```bash
agentpalace status
agentpalace search "your query"
agentpalace search "your query" --view my-feature-branch
agentpalace wake-up
```

A working `status` shows wings and rooms with drawer counts. `search` returns matching results with similarity scores — canonical rows by default, or a branch view composed over them with `--view`. `wake-up` renders your L0 + L1 context.

## 5. Connect your AI (MCP)

If you used the installer, this already happened: it ran `agentpalace setup`, which detects installed AI tools (Claude Code, Codex, Gemini, opencode, Copilot, Antigravity) and registers the `agentpalace` MCP server with each. It also warms the embedding model — downloading the assets on a fresh machine, then checking the model starts offline exactly as the MCP server will — so the very first MCP launch doesn't abort with `OfflineStartup`. Re-run it any time — it's idempotent:

```bash
agentpalace setup                     # register with every detected tool and warm the model
agentpalace setup --dry-run           # preview without writing anything or warming the model
agentpalace setup --tools claude      # restrict to a comma-separated subset
agentpalace setup --no-model-warmup   # skip the model warm-up (air-gapped, staged cache)
```

If the warm-up's offline check fails (no network on a fresh machine), `setup` exits non-zero and prints the remediation — re-run it with network access, or stage the cache yourself and use `--no-model-warmup`. When the installer runs it, that failure is a warning: the binaries are already installed, so the installer finishes and tells you exactly what to run later to complete the warm-up.

For tools `setup` doesn't cover, point them at `~/.agentpalace/bin/agentpalace` with arguments `serve --stdio` manually:

### Claude Desktop

Add to `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "agentpalace": {
      "command": "/absolute/path/to/.agentpalace/bin/agentpalace",
      "args": ["serve", "--stdio"]
    }
  }
}
```

### Claude Code

```bash
claude mcp add agentpalace -- ~/.agentpalace/bin/agentpalace serve --stdio
```

### Cline / Cursor / Any MCP host

Point your MCP client at `agentpalace` with arguments `serve --stdio`: the server speaks stdio MCP and exposes all 69 tools during `initialize`/`tools/list`.

To give different MCP clients distinct persistent selves, set `AGENTPALACE_LINEAGE_ID` in each
server registration. Lineage selection is then fixed by the host and cannot be overridden by a
model tool call. If the selected lineage does not exist yet, wake-up uses the palace default and
explains how to create the requested lineage with `agentpalace_lineage_set`. See [Self-continuity](Self-Continuity.md#binding-a-lineage-to-an-mcp-client) for Codex and OpenCode examples.

Your AI now has access to `agentpalace_search`, `agentpalace_add_drawer`, `agentpalace_kg_query`, and 66 more tools. Ask it about your project and it can search your palace on demand. The complete list, including coordination, skill-registry, delegation-telemetry, and self-continuity tools, is in [Release Scope](Release-Scope.md#mcp-tool-surface-69-tools).

## Next steps

- [Operator guide](Operator-Standard.md) — deployment, troubleshooting, storage recovery
- [CLI reference](CLI-Surface.md) — all commands and flags
- [Config schema](Config-Schema.md) — `~/.agentpalace/config.json`, `projects.json`, and optional `agentpalace.yaml`
- [Low-CPU mode](Operator-Low-CPU.md) — constrained environments
- [Federation](Federation.md) — share a palace across machines: server setup, routing, federated & branch-aware mining
- [Self-continuity](Self-Continuity.md) — preserve a reviewed agent lineage across model and harness changes
- [Hook installation](../hooks/README.md) — auto-save for Claude Code
