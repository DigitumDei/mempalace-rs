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
- An MCP server (`mempalace-mcp`) for agent integration (23 tools)
- A CLI (`mempalace-cli`) for direct palace management
- Locator-based mined storage: project file chunks store byte/line ranges instead of duplicated text; snippets are resolved lazily from the checkout at read time with stale detection
- Branch-delta mining (`mine --branch`): mines only files changed vs the merge-base with the default branch, plus untracked files — keeps the local palace in sync with ongoing branch work without re-ingesting the whole repo
- Federated project mining: when a wing's route targets a remote palace, `mine` prepares chunks locally and pushes them to the remote server via `POST /v1/ingest/batch`; the server embeds and stores them, so teams can share a single mined index without distributing embedding workload to every client
- Federation: an HTTP server (`mempalace-cli serve`) shares a palace with other clients; per-wing `local`/`remote`/`combined` routing merges remote and local results, with bearer-token auth — see the [Federation guide](docs/Federation.md)

## Crates

| Crate | Purpose |
|---|---|
| `mempalace-cli` | Command-line interface (`init`, `mine`, `search`, `status`, `wake-up`, `setup`, `serve`) |
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
| `mempalace-federation` | Shared wire DTOs for the federation REST API |
| `mempalace-server` | Axum federation REST server (`mempalace serve`) |
| `mempalace-remote` | Federation HTTP client (RemoteApi trait + RemoteClient) |

## Requirements

- Rust 1.88+
- `protobuf-compiler` (for storage layer)

## Documentation

- [Quickstart](docs/Quickstart.md) — 5-minute setup
- [Operator guide](docs/Operator-Standard.md) — deployment, troubleshooting, storage recovery
- [CLI surface](docs/CLI-Surface.md) — all commands
- [Config schema](docs/Config-Schema.md) — `~/.mempalace/config.json`
- [Low-CPU mode](docs/Operator-Low-CPU.md) — constrained environments
- [Mined storage](docs/Mined-Storage.md) — locator model, stale semantics, discovery rules
- [Federation](docs/Federation.md) — running a server, client routing, federated & branch-aware mining
- [Hook setup](hooks/README.md) — auto-save for Claude Code

## License

MIT
