# mempalace-rs — instructions for Claude

A Rust workspace of 16 crates implementing MemPalace, a local-first memory store for LLM
agents.

**The invariant to protect:** memory never leaves the user's control by default. Embeddings,
search, and the knowledge graph run locally — no third-party model or inference APIs, no
telemetry, no analytics. Don't introduce one.

Federation is the deliberate exception and is a supported product path: `mempalace-remote`
(`RemoteClient`, reqwest-backed) and `mempalace-server` speak HTTP to a palace endpoint the
user configured themselves, opt-in per wing via `local`/`remote`/`combined` routing. Work on
that path is normal work — see [docs/Federation.md](docs/Federation.md).

## Build

```bash
cargo check --workspace --all-targets --locked
```

Requires `protoc` (`protobuf-compiler`) — `mempalace-storage` pulls it in via `lancedb`.
`rusqlite` uses the `bundled` feature, so a C toolchain is needed too. On a cold box, run
`bash scripts/cloud-setup.sh`; see [docs/Cloud-Environment.md](docs/Cloud-Environment.md).

## Test

A whole-workspace `cargo test` is heavy, so CI splits it per package.
[.github/workflows/ci.yml](.github/workflows/ci.yml) is the source of truth. Work one package
at a time:

```bash
cargo test -p mempalace-storage --locked
```

Packages: `mempalace-a2a`, `mempalace-cli`, `mempalace-config`, `mempalace-core`,
`mempalace-dialect`, `mempalace-embeddings`, `mempalace-federation`, `mempalace-graph`,
`mempalace-import`, `mempalace-ingest`, `mempalace-mcp`, `mempalace-mcp-tasks`,
`mempalace-remote`, `mempalace-search`, `mempalace-server`, `mempalace-storage`.

## Embeddings are offline by default

Both binaries refuse to download model assets unless `MEMPALACE_EMBED_ALLOW_DOWNLOADS` is set
to `1`/`true`/`yes`. The test suite passes without it — don't set it to make a test go green.

- `MEMPALACE_STUB_EMBEDDINGS=1` runs with deterministic stub vectors; use it for MCP, CLI, and
  server tests that only need *an* embedding, not a real one.
- Set `MEMPALACE_EMBED_ALLOW_DOWNLOADS=1` only when deliberately exercising real model
  behaviour (e.g. `examples/embedding_bench`).

## Lints

The workspace denies `unwrap_used`, `todo`, `dbg_macro`, `undocumented_unsafe_blocks`, and
several `explicit_*`/`manual_*` lints; `unsafe_code` is **forbidden** and `missing_docs` warns.
See `[workspace.lints]` in [Cargo.toml](Cargo.toml).

The `Clippy` CI job runs this on every push and blocks the release gate. Run it yourself before
proposing changes so you find problems before CI does:

```bash
cargo clippy --workspace --all-targets --locked
```

Prefer `?`, `expect` with a real justification, or explicit error types over `unwrap`.

## Documentation must stay current

**Docs are part of the change, not a follow-up.** Every change that alters observable
behaviour ships with the matching documentation update in the *same* commit or PR. A PR that
changes behaviour and leaves the docs describing the old behaviour is incomplete — treat a
stale doc the same way you would treat a failing test.

What counts as observable behaviour, and where it is documented:

| You changed | Update |
|---|---|
| A CLI command, subcommand, flag, default, or exit code | [docs/CLI-Surface.md](docs/CLI-Surface.md) |
| An MCP tool name, argument, or response shape | [docs/Release-Scope.md](docs/Release-Scope.md) (tool list) and the tool's own `description`/`input_schema` |
| A config field, env var, default, or validation rule | [docs/Config-Schema.md](docs/Config-Schema.md) |
| A REST route, wire DTO, or capability string | [docs/Federation.md](docs/Federation.md) |
| Storage layout, source-key format, locator/view metadata, discovery rules | [docs/Mined-Storage.md](docs/Mined-Storage.md) |
| Deployment, maintenance, or recovery behaviour | [docs/Operator-Standard.md](docs/Operator-Standard.md) |
| Build/toolchain/network requirements | [docs/Cloud-Environment.md](docs/Cloud-Environment.md), [README.md](README.md) |
| The release or signing pipeline | [docs/Release-Operations.md](docs/Release-Operations.md), [docs/Packaging-And-Validation.md](docs/Packaging-And-Validation.md) |
| A new crate, or a crate's purpose | [README.md](README.md) crates table, and this file's package list |

Rules:

- **Verify against the code, not against the existing prose.** Before editing a doc, read the
  current implementation. Copied-forward text is the main way these files rot.
- **No invented surface.** Don't document a flag, field, or endpoint that does not exist, and
  don't leave links pointing at files that aren't in the repo.
- **Keep counts and lists honest.** The MCP tool count, the crate list, and the accepted-
  extension tables are all asserted in prose; update them when the code changes.
- **Dated evidence is historical.** [docs/Validation-Evidence.md](docs/Validation-Evidence.md)
  records a specific run — annotate it when its findings are resolved rather than rewriting
  the measurements.
- If a change deliberately leaves a doc gap, say so explicitly in the PR description.

## Git

- Never commit or push unless explicitly asked.
- Never push directly to `main`. Branch, then open a PR.

## Notes

- `.claude/` is gitignored, so local permission settings are per-machine and are not shared
  with other contributors or with cloud sessions.
- Release signing and immutable-release publishing live in `release/` and are CI-only; never
  run them locally or add release secrets to a dev or cloud environment. See
  [docs/Release-Operations.md](docs/Release-Operations.md).
