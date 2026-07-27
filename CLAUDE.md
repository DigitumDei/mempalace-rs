# mempalace-rs — instructions for Claude

A Rust workspace of 14 crates implementing MemPalace, a local-first memory store for LLM
agents. Everything runs on the local machine — there are no external API calls in the product
path, and changes should keep it that way.

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

Packages: `mempalace-cli`, `mempalace-config`, `mempalace-core`, `mempalace-dialect`,
`mempalace-embeddings`, `mempalace-federation`, `mempalace-graph`, `mempalace-import`,
`mempalace-ingest`, `mempalace-mcp`, `mempalace-remote`, `mempalace-search`,
`mempalace-server`, `mempalace-storage`.

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

No CI job runs clippy, so it is easy to regress. Check it yourself before proposing changes:

```bash
cargo clippy --workspace --all-targets --locked
```

Prefer `?`, `expect` with a real justification, or explicit error types over `unwrap`.

## Git

- Never commit or push unless explicitly asked.
- Never push directly to `main`. Branch, then open a PR.

## Notes

- `.claude/` is gitignored, so local permission settings are per-machine and are not shared
  with other contributors or with cloud sessions.
- Release signing and immutable-release publishing live in `release/` and are CI-only; never
  run them locally or add release secrets to a dev or cloud environment. See
  [docs/Release-Operations.md](docs/Release-Operations.md).
