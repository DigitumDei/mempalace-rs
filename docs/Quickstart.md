# Quickstart

Get MemPalace running and connected to your AI in a few minutes.

## 1. Install

The installer downloads the latest stable binaries for your platform, verifies the signed release manifest and the artifact digests, installs them to `~/.mempalace/bin`, adds that directory to your PATH, and runs `mempalace-cli setup` to register the MCP server with detected AI tools.

**macOS (Apple Silicon) / Linux (x86_64, glibc 2.38+):**

```bash
curl -fsSL https://raw.githubusercontent.com/DigitumDei/mempalace-rs/main/install.sh | sh
```

**Windows (x86_64, PowerShell 7.1+):**

```powershell
irm https://raw.githubusercontent.com/DigitumDei/mempalace-rs/main/install.ps1 | iex
```

Options:

| sh flag | ps1 parameter | Effect |
|---|---|---|
| `--no-setup` | `-NoSetup` (or `$env:MEMPALACE_NO_SETUP='1'`) | skip MCP registration |
| `--no-path` | `-NoPath` (or `$env:MEMPALACE_NO_PATH='1'`) | don't touch your PATH |
| `--install-dir <dir>` | `-InstallDir <dir>` (or `$env:MEMPALACE_INSTALL_DIR`) | install elsewhere |
| `--channel nightly --version nightly-<full-commit-sha>` | `-Channel nightly -Version nightly-<full-commit-sha>` | explicitly install an immutable test candidate |

Pass sh flags through the pipe with `| sh -s -- --no-setup`. For PowerShell parameters, download the script first (`irm ... -OutFile install.ps1`) — env vars work with the piped one-liner.

Stable releases are immutable and are the default installer target. Candidates from `main` are immutable `nightly-<full-commit-sha>` prereleases and require the explicit nightly channel and version above.
The installer never falls back to the legacy mutable `nightly` tag. Before the first stable release exists, it exits without changing an existing installation.

The Windows installer verifies release signatures with cryptography APIs introduced in PowerShell 7.1. Install PowerShell 7.1 or later before using the Windows command above.

Supported platforms: Linux x86_64 (glibc 2.38+), macOS Apple Silicon, Windows x86_64. Anything else (Intel macOS, ARM Linux, musl) has no prebuilt ONNX Runtime — build from source instead.

### 1b. Build from source (alternative)

```bash
cargo build --release -p mempalace-cli -p mempalace-mcp
```

Requires Rust 1.88+ and `protobuf-compiler` (see [README](../README.md)).

Expected binaries:
- `target/release/mempalace-cli`
- `target/release/mempalace-mcp`

The rest of this guide assumes `mempalace-cli` is on your PATH (the installer does this); for a source build, substitute `./target/release/mempalace-cli`.

## 2. Initialize a palace

Point `init` at any project directory. It detects folders and stores the wing
and room configuration in the local registry (`~/.mempalace/projects.json` by
default), so no repository file is required:

```bash
mempalace-cli init /path/to/your/project
```

Use `--repo-config` when you explicitly want a portable repository-local
`mempalace.yaml` as well.

For a repository without a Git `origin`, provide a durable identity explicitly
so another checkout can resolve the same declaration:

```bash
./target/release/mempalace-cli init /path/to/your/project --project-id local/my-project
```

On first run, you'll see a startup validation status. If it's not `ready`, set `MEMPALACE_EMBED_ALLOW_DOWNLOADS=1` to download embedding assets, then re-run `init`.

## 3. Ingest data

```bash
# Mine project files (code, docs, notes)
mempalace-cli mine /path/to/your/project

# Mine conversation exports
mempalace-cli mine /path/to/chats/ --mode convos --wing project_name
```

## 4. Verify it works

```bash
mempalace-cli status
mempalace-cli search "your query"
mempalace-cli wake-up
```

A working `status` shows wings and rooms with drawer counts. `search` returns matching results with similarity scores. `wake-up` renders your L0 + L1 context.

## 5. Connect your AI (MCP)

If you used the installer, this already happened: it ran `mempalace-cli setup`, which detects installed AI tools (Claude Code, Codex, Gemini, opencode, Copilot, Antigravity) and registers the `mempalace` MCP server with each. Re-run it any time — it's idempotent:

```bash
mempalace-cli setup            # add --dry-run to preview, --only claude to restrict
```

For tools `setup` doesn't cover, point them at `~/.mempalace/bin/mempalace-mcp` manually:

### Claude Desktop

Add to `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "mempalace": {
      "command": "/absolute/path/to/.mempalace/bin/mempalace-mcp"
    }
  }
}
```

### Claude Code

```bash
claude mcp add mempalace -- ~/.mempalace/bin/mempalace-mcp
```

### Cline / Cursor / Any MCP host

Point your MCP client at the `mempalace-mcp` binary. No arguments needed — the server speaks stdio MCP and exposes all 23 tools on `initialize`.

Your AI now has access to `mempalace_search`, `mempalace_add_drawer`, `mempalace_kg_query`, and 16 more tools. Ask it anything about your project — it can search your palace on demand.

## Next steps

- [Operator guide](Operator-Standard.md) — deployment, troubleshooting, storage recovery
- [CLI reference](CLI-Surface.md) — all commands and flags
- [Config schema](Config-Schema.md) — `~/.mempalace/config.json`, `projects.json`, and optional `mempalace.yaml`
- [Low-CPU mode](Operator-Low-CPU.md) — constrained environments
- [Federation](Federation.md) — share a palace across machines: server setup, routing, federated & branch-aware mining
- [Hook installation](../hooks/README.md) — auto-save for Claude Code
