<div align="center">
  <img src="docs/mempalace_logo.png" alt="MemPalace" width="280">
</div>

# mempalace-rs

Rust implementation of MemPalace — a persistent, structured memory system for LLM agents.

MemPalace stores conversation and project context locally so your AI can search decisions, debugging history, and project knowledge instead of starting from zero each session. No external API calls, no cloud — everything runs on your machine.

## Quick start

```bash
cargo build --release -p mempalace-cli -p mempalace-mcp
./target/release/mempalace-cli init /path/to/project
./target/release/mempalace-cli mine /path/to/project
```

Then point your MCP client at `./target/release/mempalace-mcp`.

See the [Quickstart guide](docs/Quickstart.md) for the full walkthrough — building, initializing, mining, searching, and connecting to Claude/Cursor/Cline.

## Overview

MemPalace provides a palace-style memory store with:

- Semantic search via local embeddings (no external API calls)
- A knowledge graph for structured facts, relationships, and timelines
- An AAAK dialect for compact, human-readable memory storage
- An MCP server (`mempalace-mcp`) for agent integration (19 tools)
- A CLI (`mempalace-cli`) for direct palace management

## Crates

| Crate | Purpose |
|---|---|
| `mempalace-cli` | Command-line interface (`init`, `mine`, `search`, `status`, `wake-up`) |
| `mempalace-mcp` | MCP server for agent tool integration |
| `mempalace-core` | Core types and traits |
| `mempalace-storage` | Palace persistence layer |
| `mempalace-ingest` | Content ingestion and chunking |
| `mempalace-embeddings` | Local embedding model (no external API) |
| `mempalace-search` | Semantic search |
| `mempalace-graph` | Knowledge graph |
| `mempalace-config` | Configuration management |
| `mempalace-dialect` | AAAK dialect encoding/decoding |
| `mempalace-import` | Migration from Python palace state |

## Requirements

- Rust 1.88+
- `protobuf-compiler` (for storage layer)

## Documentation

- [Quickstart](docs/Quickstart.md) — 5-minute setup
- [Operator guide](docs/Operator-Standard.md) — deployment, troubleshooting, storage recovery
- [CLI surface](docs/CLI-Surface.md) — all commands
- [Config schema](docs/Config-Schema.md) — `~/.mempalace/config.json`
- [Low-CPU mode](docs/Operator-Low-CPU.md) — constrained environments
- [Hook setup](hooks/README.md) — auto-save for Claude Code

## License

MIT
