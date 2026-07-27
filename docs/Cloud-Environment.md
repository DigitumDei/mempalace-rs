# Cloud Environment

Everything needed to stand up a cloud sandbox — Claude Code on the web, a CI runner, or any
fresh Linux box — that can build and test this workspace.

The short version — on Ubuntu 24.04, with the domains below reachable:

```bash
bash scripts/cloud-setup.sh
```

The rest of this page explains what that script assumes.

## Base image

Ubuntu 24.04 (glibc 2.38 or newer). The floor comes from the prebuilt ONNX Runtime the build
links against, the same constraint that limits our Linux release target.

- `x86_64` and `aarch64` both work — prebuilt ONNX Runtime exists for
  `x86_64-unknown-linux-gnu` and `aarch64-unknown-linux-gnu`.
- **musl is not supported.** No prebuilt ONNX Runtime is published for it; a musl box would
  need ONNX Runtime compiled from source and linked manually.

## Sizing

675 crates in [`Cargo.lock`](../Cargo.lock), dominated by `lancedb`, `arrow`, and `ort`.

| Resource | Recommended |
|---|---|
| vCPU | 4+ |
| RAM | 8 GB |
| Disk | 30 GB |

The cold `cargo check --workspace --all-targets` is the long pole. `[profile.dev]` sets
`debug = 0`, `incremental = false`, and `strip = "debuginfo"`, which keeps `target/` far
smaller than a stock debug profile — but it still grows quickly once you add release builds or
extra target triples, so give the box headroom rather than the minimum.

## System packages

```
build-essential ca-certificates curl git libssl-dev pkg-config protobuf-compiler
```

Three of these are load-bearing in ways the crate names don't advertise:

- **`protobuf-compiler`** (`protoc`) — required by `mempalace-storage` through
  `lancedb` → `lance`. Without it `cargo check --workspace` fails outright.
- **`build-essential`** — `rusqlite` uses the `bundled` feature, so SQLite is compiled from
  C source on every fresh machine.
- **`libssl-dev`** — `openssl-sys` is *not* vendored (there is no `openssl-src` in
  `Cargo.lock`), so it links the system OpenSSL through `pkg-config`. A minimal image that
  ships only the OpenSSL runtime has neither the headers nor the `.pc` file, and neither
  `build-essential` nor `pkg-config` supplies them.

## Toolchain

Rust **stable, 1.88 minimum** (`edition = "2024"`, `rust-version = "1.88"`). There is no
`rust-toolchain.toml`, so the box's default stable is what you get; the setup script installs
and pins stable explicitly, plus the `clippy` and `rustfmt` components.

## Network allowlist

If the environment has an egress firewall, these are the domains that matter. There are **zero
git-sourced dependencies** in `Cargo.lock`, so the build needs nothing beyond this list.

| Domain | Why | Required |
|---|---|---|
| `crates.io`, `index.crates.io`, `static.crates.io` | Cargo registry index and crate downloads | Yes |
| `cdn.pyke.io` | Prebuilt ONNX Runtime, fetched by the `ort-sys` build script | **Yes — the build fails without it** |
| Your image's APT mirror — see below | `apt-get install` of the packages above | Yes, unless baked into the image |
| `static.rust-lang.org`, `sh.rustup.rs` | rustup and toolchain downloads | Only if Rust is absent or needs pinning |
| `github.com`, `api.github.com`, `objects.githubusercontent.com` | `gh` CLI — PRs, CI logs | Only if the session uses `gh` |
| `huggingface.co`, `cdn-lfs.huggingface.co`, `cdn-lfs-us-1.hf.co` | Embedding model weights, at runtime | **No** — see below |

**Don't assume the canonical Ubuntu mirrors.** Cloud images usually point APT at a provider or
regional mirror — `azure.archive.ubuntu.com`, `<region>.ec2.archive.ubuntu.com`,
`ports.ubuntu.com` on arm64, and so on. Read the actual hosts off the image rather than
allowlisting `archive.ubuntu.com` and hoping:

```bash
grep -rhoE 'https?://[^ ]+' /etc/apt/sources.list /etc/apt/sources.list.d/ 2>/dev/null \
  | sed -E 's|https?://([^/]+).*|\1|' | sort -u
```

Allow every host that prints. (Alternatively, rewrite the image's sources to the canonical
hosts, or bake the packages into the image and skip APT egress entirely.)

`cdn.pyke.io` is the one people miss. `ort-sys` downloads a hash-verified ONNX Runtime tarball
from there (e.g.
`https://cdn.pyke.io/0/pyke:ort-rs/ms@1.23.2/x86_64-unknown-linux-gnu.tar.lzma2`) and caches it
under `~/.cache/ort.pyke.io`. If it is blocked, the build script reports
`ort-sys failed to download prebuilt binaries from ...` — an explicit error, not a hang.

The HuggingFace domains are deliberately omitted: the workspace's tests pass without ever
downloading a model (see below), so leaving them off keeps the sandbox's egress surface small.
Add them only if you intend to run the embedding benchmarks.

## Environment variables

| Variable | Value | Why |
|---|---|---|
| `CARGO_TERM_COLOR` | `always` | Matches CI |
| `CARGO_NET_RETRY` | `5` | A cold 675-crate fetch is unforgiving of flaky sandbox networking |
| `RUST_BACKTRACE` | `1` | Useful default when a test fails |
| `MEMPALACE_EMBED_CACHE` | `$HOME/.cache/mempalace/embeddings` | Matches the CI layout |
| `MEMPALACE_STUB_EMBEDDINGS` | `1` | Deterministic stub vectors, so MCP/CLI/server tests run fully offline |

**Do not set `MEMPALACE_EMBED_ALLOW_DOWNLOADS`.** Both binaries default to offline and refuse to
fetch model assets without it, and the test suite is green in that state — CI's
`embeddings-tests` job never sets it, and only the separate `embedding-baselines` job does.
Leaving it unset is what allows the HuggingFace domains to stay off the allowlist. If you need
real model behaviour (e.g. `cargo run -p mempalace-embeddings --example embedding_bench`), set
it for that command and add the HuggingFace domains for that environment.

## Secrets

**None are required to build or test.**

- `MEMPALACE_RELEASE_SIGNING_KEY` is CI-only, used for signing release manifests. It must never
  be placed in a development or cloud sandbox environment.
- `GH_TOKEN` is only needed if the session should open pull requests or read CI logs via `gh`.

## Setting up Claude Code on the web

1. Set the environment's setup command to `bash scripts/cloud-setup.sh`.
2. Add the "Yes" domains from the allowlist above to the environment's firewall configuration.
3. Set the environment variables above.
4. Take the snapshot **after** the setup script completes, so `~/.cargo/registry`,
   `~/.cache/ort.pyke.io`, and `target/` are baked in. Without this, every session pays the
   cold-build cost.

[`CLAUDE.md`](../CLAUDE.md) at the repo root is picked up automatically and gives the session
the per-package test split, the lint policy, and the offline-embeddings rule.

Note that `.claude/` is gitignored, so local permission allowlists do not travel to the cloud
environment; expect a fresh session to ask for approval on commands until you configure it.

## Verifying a new environment

```bash
cargo check --workspace --all-targets --locked
cargo test -p mempalace-embeddings --locked
cargo test -p mempalace-storage --locked
cargo clippy --workspace --all-targets --locked
```

Those two packages are the environment-sensitive ones: `mempalace-embeddings` proves the ONNX
Runtime download and the offline model path both work, and `mempalace-storage` proves `protoc`
and the bundled SQLite build are present. The clippy run confirms the component was installed.
