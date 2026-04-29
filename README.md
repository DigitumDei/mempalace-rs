# mempalace-rs

Rust implementation of MemPalace — a persistent, structured memory system for LLM agents.

## Overview

MemPalace provides a palace-style memory store with:
- Semantic search via local embeddings (no external API calls)
- A knowledge graph for structured facts, relationships, and timelines
- An AAAK dialect for compact, human-readable memory storage
- An MCP server (`mempalace-mcp`) for agent integration
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

## Build

```sh
cargo build --release -p mempalace-cli -p mempalace-mcp
```

## Agent integration (MCP)

See [`hooks/README.md`](hooks/README.md) for Claude Code hook setup and [`docs/`](docs/) for operator configuration guides.

## License

MIT
