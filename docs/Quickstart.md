# Quickstart

Get MemPalace running and connected to your AI in a few minutes.

## 1. Build

```bash
cargo build --release -p mempalace-cli -p mempalace-mcp
```

Requires Rust 1.88+ and `protobuf-compiler` (see [README](../README.md)).

Expected binaries:
- `target/release/mempalace-cli`
- `target/release/mempalace-mcp`

## 2. Initialize a palace

Point `init` at any project directory. It detects folders and creates a `mempalace.yaml` room config:

```bash
target/release/mempalace-cli init /path/to/your/project
```

On first run, you'll see a startup validation status. If it's not `ready`, set `MEMPALACE_EMBED_ALLOW_DOWNLOADS=1` to download embedding assets, then re-run `init`.

## 3. Ingest data

```bash
# Mine project files (code, docs, notes)
target/release/mempalace-cli mine /path/to/your/project

# Mine conversation exports
target/release/mempalace-cli mine /path/to/chats/ --mode convos --wing project_name
```

## 4. Verify it works

```bash
target/release/mempalace-cli status
target/release/mempalace-cli search "your query"
target/release/mempalace-cli wake-up
```

A working `status` shows wings and rooms with drawer counts. `search` returns matching results with similarity scores. `wake-up` renders your L0 + L1 context.

## 5. Connect your AI (MCP)

### Claude Desktop

Add to `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "mempalace": {
      "command": "/absolute/path/to/target/release/mempalace-mcp"
    }
  }
}
```

### Claude Code

```bash
claude mcp add mempalace -- /absolute/path/to/target/release/mempalace-mcp
```

### Cline / Cursor / Any MCP host

Point your MCP client at the `mempalace-mcp` binary. No arguments needed — the server speaks stdio MCP and exposes all 19 tools on `initialize`.

Your AI now has access to `mempalace_search`, `mempalace_add_drawer`, `mempalace_kg_query`, and 16 more tools. Ask it anything about your project — it searches your palace automatically.

## Next steps

- [Operator guide](Operator-Standard.md) — deployment, troubleshooting, storage recovery
- [CLI reference](CLI-Surface.md) — all commands and flags
- [Config schema](Config-Schema.md) — `~/.mempalace/config.json` and `mempalace.yaml`
- [Low-CPU mode](Operator-Low-CPU.md) — constrained environments
- [Hook installation](../hooks/README.md) — auto-save for Claude Code
