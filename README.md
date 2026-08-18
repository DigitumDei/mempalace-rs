<div align="center">
  <img src="docs/assets/mempalace-rs-logo.png" alt="mempalace-rs" width="280">
</div>

# mempalace-rs

Rust implementation of MemPalace — a persistent, structured memory system for LLM agents.

MemPalace stores conversation and project context locally so your AI can search decisions, debugging history, and project knowledge instead of starting from zero each session. No external API calls, no cloud — everything runs on your machine.

## Quick start

Install the latest stable build — downloads the signed release manifest and binaries for your platform, verifies both before installing to `~/.mempalace/bin`, adds it to your PATH, and registers the MCP server with detected AI tools (Claude Code, Codex, Gemini, and more):

**macOS (Apple Silicon) / Linux (x86_64, glibc 2.38+):**

```bash
curl -fsSL https://raw.githubusercontent.com/DigitumDei/mempalace-rs/main/install.sh | sh
```

**Windows (x86_64):**

```powershell
irm https://raw.githubusercontent.com/DigitumDei/mempalace-rs/main/install.ps1 | iex
```

Then create and fill a palace for a project:

```bash
mempalace-cli init /path/to/project
mempalace-cli mine /path/to/project
```

Stable releases are immutable. Test a candidate only with the explicit `--channel nightly --version v<version>-nightly.<full-commit-sha>` installer options. Other platforms (Intel macOS, ARM Linux, musl) need a [source build](docs/Quickstart.md#1b-build-from-source-alternative).

See the [Quickstart guide](docs/Quickstart.md) for the full walkthrough — installing, initializing, mining, searching, and connecting to Claude/Cursor/Cline.
Release operators should follow the [signed release runbook](docs/Release-Operations.md).

## Overview

MemPalace provides a palace-style memory store with:

- Semantic search via local embeddings (no external API calls)
- A knowledge graph for structured facts, relationships, and timelines
- An AAAK dialect for compact, human-readable memory storage
- An MCP server (`mempalace-mcp`) for agent integration (28 tools)
- Provider-neutral agent lineages and reviewed identity packets that preserve a coherent self
  across model and harness changes
- A CLI (`mempalace-cli`) for direct palace management
- Locator-based mined storage: project file chunks store byte/line ranges instead of duplicated text; snippets are resolved lazily from the checkout at read time with stale detection
- Repository views: `mine` detects the checkout automatically — a full canonical snapshot on the default branch, a branch delta everywhere else — and `search --view <branch>` composes that delta over the canonical snapshot, tombstones included. Force either side with `--full` / `--branch`
- Scoped pruning (`mine`'s inverse): `prune` previews and then deletes mined project data by project, wing, ingest kind, branch view, or path prefix, local palace only
- Background maintenance: fragment compaction, version retention, and vector-index optimization, run automatically by the hub and on demand via `mempalace-cli maintain`
- Federated project mining: when a wing's route targets a remote palace, `mine` prepares chunks locally and pushes them to the remote server via `POST /v1/ingest/batch`; the server embeds and stores them, so teams can share a single mined index without distributing embedding workload to every client
- Federation: an HTTP server (`mempalace-cli serve`) shares a palace with other clients; per-wing `local`/`remote`/`combined` routing merges remote and local results, with bearer-token auth and `write: both` local-first dual-write support — see the [Federation guide](docs/Federation.md)

## Crates

| Crate | Purpose |
|---|---|
| `mempalace-cli` | Command-line interface (`init`, `mine`, `project`, `prune`, `search`, `status`, `wake-up`, `setup`, `maintain`, `serve`) |
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

These apply only when building from source — the prebuilt nightly install above needs none of them (including the corporate-SSL configuration below).

- Rust 1.88+
- `protobuf-compiler` (for storage layer)

(ONNX Runtime is downloaded automatically by the build — see the note below if you build behind an SSL-inspecting proxy.)

## Building behind corporate SSL inspection (Netskope, Zscaler, etc.)

Corporate network proxies that perform SSL inspection (Netskope, Zscaler, and similar) intercept TLS connections and re-sign them with a custom root CA. Several build-time dependencies download binaries using their own TLS stacks, which won't trust that CA by default. The embeddings crate is configured to download over the OS **native TLS** stack so it trusts your proxy's CA automatically (see section 3); the only thing you normally need to configure is Cargo itself.

The commands and paths below are written for Windows (where native TLS means Schannel and the system certificate store), but the same concepts apply on macOS (Keychain / Security framework) and Linux (OpenSSL reading the system trust store, e.g. `/etc/ssl/certs`) — substitute your platform's CA bundle path and trust store accordingly.

### 1. Cargo HTTP (crates.io index and crate downloads)

Create or edit `~/.cargo/config.toml`:

```toml
[http]
cainfo = "C:/ProgramData/Netskope/stagent/download/nscacert_combined.pem"
check-revoke = false
```

`cainfo` points Cargo's HTTP client at your proxy's CA bundle. `check-revoke` disables certificate revocation checks, which typically time out through an intercepting proxy. Adjust the path to match your proxy's CA bundle — for Zscaler it's usually somewhere under `C:/Program Files/Zscaler/`.

> **Security note:** `check-revoke = false` stops Cargo from checking whether a certificate has been revoked (e.g. after a key compromise), so set it only because the intercepting proxy makes revocation checks unreliable, and scope it to trusted corporate networks. If your proxy's revocation endpoints are reachable, leave it at the default (`true`).

If you don't have the PEM path, open `certmgr.msc`, find your proxy's root certificate under Trusted Root Certification Authorities, export it as Base-64 encoded X.509 (.cer), and use that path.

### 2. ONNX Runtime (downloaded by the embeddings layer's build script)

The `ort-sys` build script downloads a prebuilt ONNX Runtime binary from `cdn.pyke.io` (for example `https://cdn.pyke.io/0/pyke:ort-rs/ms@1.23.2/x86_64-pc-windows-msvc.tar.lzma2`), verifies its hash, and caches it under your local cache directory in `ort.pyke.io/dfbin`. The embeddings crate enables fastembed's `ort-download-binaries-native-tls` feature, so this download uses the OS **native TLS** stack (Windows Schannel) and trusts your proxy's CA from the system certificate store. As long as that root certificate is installed (which Netskope and Zscaler both do by default), **no configuration is needed** — the default rustls-based variant, which ignores the system store and fails behind such a proxy, is not used.

If your firewall filters by domain rather than inspecting TLS, allow `cdn.pyke.io` — the build cannot complete without it.

**Offline / air-gapped fallback.** If the build can't reach `cdn.pyke.io` at all (rather than a TLS-trust problem), download a matching ONNX Runtime release once and point the build at it. Using PowerShell:

```powershell
Invoke-WebRequest -Uri "https://github.com/microsoft/onnxruntime/releases/download/v1.23.2/onnxruntime-win-x64-1.23.2.zip" -OutFile "$env:TEMP\onnxruntime.zip"
Expand-Archive -Path "$env:TEMP\onnxruntime.zip" -DestinationPath "C:\onnxruntime" -Force
```

Then add this to `~/.cargo/config.toml` so every build finds it (this overrides the auto-download):

```toml
[env]
ORT_LIB_LOCATION = "C:/onnxruntime/onnxruntime-win-x64-1.23.2/lib"
```

Copy `onnxruntime.dll` and `onnxruntime_providers_shared.dll` from that `lib` directory alongside your built binaries when deploying to another machine.

### 3. HuggingFace model downloads (embedding models at runtime)

The embeddings layer downloads model files from HuggingFace Hub on first run. Two TLS configurations make this work through SSL inspection:

1. **fastembed** is configured with `hf-hub-native-tls`, so the main download client uses the Windows certificate store and trusts your proxy's CA.
2. **ureq** (the HTTP client used internally by `hf-hub`) has the `native-certs` feature enabled, which makes its rustls TLS backend also load root certificates from the Windows certificate store. Without this, ureq's default rustls would only trust bundled WebPKI roots and reject the proxy's re-signed certificates.

Both are already configured in the workspace — no additional setup is needed as long as your proxy's root certificate is installed in the Windows system certificate store (which Netskope and Zscaler both do by default).

## Documentation

Full index: [docs/README.md](docs/README.md).

- [Quickstart](docs/Quickstart.md) — 5-minute setup
- [Operator guide](docs/Operator-Standard.md) — deployment, maintenance, troubleshooting, storage recovery
- [CLI surface](docs/CLI-Surface.md) — all commands and flags
- [Config schema](docs/Config-Schema.md) — `~/.mempalace/config.json`
- [Release scope](docs/Release-Scope.md) — what ships, what's deferred, the 28 MCP tools
- [Self-continuity](docs/Self-Continuity.md) — lineages, reviewed self-observations, identity
  packets, and model/harness migrations
- [Low-CPU mode](docs/Operator-Low-CPU.md) — constrained environments
- [Cloud environment](docs/Cloud-Environment.md) — building and testing in a cloud sandbox or CI runner
- [Mined storage](docs/Mined-Storage.md) — locator model, repository views, stale semantics, discovery rules
- [Federation](docs/Federation.md) — running a server, client routing, federated & branch-aware mining
- [Coordination Phase 0](docs/Coordination-Phase-0.md) — experimental durable coordination skill and design findings
- [Release operations](docs/Release-Operations.md) — signed candidate and stable release runbook
- [Hook setup](hooks/README.md) — auto-save for Claude Code

Contributors: documentation is updated in the same change as the behaviour it describes — see
[CLAUDE.md](CLAUDE.md#documentation-must-stay-current).

## License

MIT
