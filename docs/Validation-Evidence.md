# Validation Evidence

This document records the Phase 12 runtime-validation evidence gathered on branch `ask/558-1776710642523` on 2026-04-20 and 2026-04-21. It is the small-VM runtime row from [Packaging-And-Validation.md](Packaging-And-Validation.md). It does not replace the GitHub Actions `build-and-package` row.

## Environment

- Host: Windows 11 Pro with Smart App Control / WDAC active.
- Cargo was run inside WSL Ubuntu-24.04 against `/mnt/d/SourceCode/mempalace/mempalace-rs`. WDAC blocks freshly-linked `build-script-build.exe` binaries on the Windows side (`os error 4551`), so Windows-native cargo was not usable.
- Toolchain: rustup stable (rustc 1.95.0, cargo 1.95.0).
- protoc 3.21.12 from Ubuntu `protobuf-compiler`.
- Logs: `validation-logs/` at the repo root.

## Commands

All cargo invocations were issued from `mempalace-rs/` and used `--locked --message-format=short`. `--message-format=short` is required as a local workaround for a rustc 1.95 ICE in `annotate_snippets::renderer::styled_buffer::replace` that fires only under certain terminal widths; CI does not hit it. See "Known blockers and workarounds" below.

This evidence predates the workflow-level `build-and-package` release job. For final release signoff, equivalent runtime checks should be re-run on this VM using the binaries uploaded by that GitHub Actions job rather than locally built artifacts.

### Workspace build

```
cargo check --workspace --all-targets --locked --message-format=short
```

Result: clean compile of the full workspace in 2m 47s. Lint warnings only (see findings). Log: `validation-logs/cargo-check.log`.

### Per-crate tests

CI mirrors, one invocation per crate:

```
cargo test -p <crate> --locked --message-format=short -- --nocapture
```

| Crate | Tests | Time |
| --- | --- | --- |
| `mempalace-embeddings` | 22/22 | 0.03s |
| `mempalace-storage` | 19/19 | 52.41s |
| `mempalace-ingest` | 16/16 | 1.06s |
| `mempalace-graph` | 16/16 | 2.17s |
| `mempalace-mcp` | 4/4 | 0.98s |
| `mempalace-cli` | 19/19 | 1.70s |
| **Total** | **96/96** | |

Logs: `validation-logs/test-*.log`, summary in `validation-logs/test-summary.txt`. Runner script: `validation-logs/run-per-crate-tests.sh`.

### Release artifacts

```
cargo build --release -p mempalace-cli -p mempalace-mcp --locked --message-format=short
```

Result: both binaries built in 11m 29s.

- `target/release/mempalace-cli` — 230,973,488 bytes
- `target/release/mempalace-mcp` — 230,579,016 bytes

Log: `validation-logs/release-build.log`.

### Install-flow smoke

Driver: `validation-logs/smoke-test.sh`. Log: `validation-logs/smoke.log`.

Against an isolated palace root and a three-file fixture (`notes/welcome.md`, `notes/overview.md`, `planning/roadmap.md`):

| Step | Command | Exit |
| --- | --- | --- |
| 1 | `mempalace-cli --help` | 0 |
| 2 | `mempalace-cli --palace <root> init --yes <fixture>` | 0 (`startup_validation=ready` against warm cache) |
| 3 | `mempalace-cli --palace <root> mine <fixture>` | 0 |
| 4 | `mempalace-cli --palace <root> search "roadmap"` | 0 |
| 5 | `mempalace-cli --palace <root> status` | 0 |
| 6 | `mempalace-cli --palace <root> wake-up` | 0 |
| 7 | `mempalace-mcp` receives `initialize` + `tools/list` on stdio | 0 |

The MCP response advertised `protocolVersion: 2024-11-05` and all 19 frozen v1 tools listed in [Release-Scope.md](Release-Scope.md): `mempalace_status`, `mempalace_list_wings`, `mempalace_list_rooms`, `mempalace_get_taxonomy`, `mempalace_get_aaak_spec`, `mempalace_kg_query`, `mempalace_kg_add`, `mempalace_kg_invalidate`, `mempalace_kg_timeline`, `mempalace_kg_stats`, `mempalace_traverse`, `mempalace_find_tunnels`, `mempalace_graph_stats`, `mempalace_search`, `mempalace_check_duplicate`, `mempalace_add_drawer`, `mempalace_delete_drawer`, `mempalace_diary_write`, `mempalace_diary_read`.

### Indicative benchmark (not signoff)

Driver: `validation-logs/run-bench.sh` running the `mempalace-embeddings` `embedding_bench` example per the CI `embedding-baselines` job, with `MEMPALACE_EMBED_ALLOW_DOWNLOADS=1` and `MEMPALACE_EMBED_ITERATIONS=15`.

| Profile | Warm p95 |
| --- | --- |
| `balanced` | 14.43 ms |
| `low_cpu` | 14.92 ms |

Logs: `validation-logs/bench-balanced.log`, `validation-logs/bench-low-cpu.log`. Populated model cache: 110 MB across `models--Xenova--all-MiniLM-L6-v2/` (balanced) and `models--Qdrant--all-MiniLM-L6-v2-onnx/` (low_cpu quantized).

These numbers were taken on the target small VM under a Windows/WSL setup with cross-FS I/O, but they were produced from locally built binaries rather than the GitHub Actions packaging row. The two profiles showing near-identical p95 on this host is expected when the VM has enough CPU headroom that the balanced profile is not the bottleneck; final release claims for `low_cpu` should be attached to the artifact-based rerun.

## Known blockers and workarounds

### 1. WDAC / Smart App Control on Windows

Symptom: `error: failed to run custom build command … An Application Control policy has blocked this file. (os error 4551)` on unsigned `build-script-build.exe` binaries emitted by cargo.

Applies to: Windows-native cargo (both Git Bash and PowerShell parents).

Workaround used: run cargo inside WSL Ubuntu-24.04 against `/mnt/d/...`. WDAC does not apply to Linux ELF binaries under WSL.

### 2. rustc 1.95 ICE in `annotate_snippets` renderer

Symptom:

```
thread 'rustc' panicked at library/core/src/slice/index.rs:1031:55:
slice index starts at 9 but ends at 8
#0 [lint_mod] linting top-level module
#1 [analysis] running analysis passes on crate `mempalace_embeddings`
```

Applies to: rustc 1.94.0 and 1.95.0 on this WSL host when rendering `missing_docs` warnings against `crates/mempalace-embeddings/src/lib.rs`. The panic site is inside the source-line truncation path in the ANSI diagnostic renderer, triggered only at certain terminal widths. CI (`dtolnay/rust-toolchain@stable`, same rustc 1.95.0) does not hit it.

Workaround used: `--message-format=short` on all cargo invocations. rustc then bypasses `annotate_snippets` and emits `file:line:col: message`.

This is an upstream rustc bug, not a repo defect. A reproducer could be filed upstream if the behavior persists on a cleaner WSL install.

## Windows-native build attempt (2026-04-21)

A follow-up pass attempted to build natively on the Windows host (cargo 1.93.0, rustc 1.93.0) to complement the WSL artifacts.

### What worked

| Step | Result |
| --- | --- |
| `cargo check --workspace --all-targets --locked --message-format=short` | Clean, 1m 44s. Same warnings as WSL run. |
| `cargo test -p <crate> --locked --message-format=short` × 6 crates | **96/96 passing.** All crates green on Windows native. |

`cargo check` and `cargo test` succeed because both operate against the dev-profile build-script cache, which is populated on the first run and not re-executed unless inputs change.

### What failed

```
cargo build --release -p mempalace-cli -p mempalace-mcp --locked --message-format=short
```

Failed with:

```
error: failed to run custom build command for `num-traits v0.2.19`
could not execute process `target\release\build\num-traits-...\build-script-build` (never executed)
An Application Control policy has blocked this file. (os error 4551)
```

The release profile uses a separate `target/release/build/` tree. Every build script is recompiled into a fresh unsigned `.exe` on first use, which Smart App Control blocks.

### Root cause: Smart App Control is On

Confirmed via registry:

```
VerifiedAndReputablePolicyState       = 1  (On)
UsermodeCodeIntegrityPolicyEnforcementStatus = 2  (Enforced)
```

Smart App Control blocks unsigned PE executables that lack cloud reputation. Cargo's build-script outputs are unsigned and newly compiled — they have no reputation. Enabling Windows Developer Mode did not affect this.

There is no "signed toolchain" workaround: the Rust project does not sign build-script outputs, and cargo provides no hook to sign them at emit time.

### Cross-compilation path (not yet completed)

An alternative is to cross-compile Windows `.exe` binaries from WSL using the `x86_64-pc-windows-gnu` target and `gcc-mingw-w64-x86-64`. The Rust std component for that target has been added (`rustup target add x86_64-pc-windows-gnu`) but `mingw-w64` was not yet installed on the WSL instance when this pass was paused. This path remains to be validated.

### Summary

Windows-native is usable for the dev/test loop (check + all tests). Release builds require either WSL (already validated) or the cross-compilation path above.

## Findings to surface for release planning

1. **Declared MSRV is stale.** `mempalace-rs/Cargo.toml` declares `rust-version = "1.85"`. The locked dependency graph actually requires rustc ≥ 1.88 (`ort-sys`, `time`, `serde_with` and derived) with 1.86 minimums in several `datafusion` and `icu_*` crates. The workspace should either bump `rust-version` to match reality or pin compatible versions.
2. **Accumulating lint warnings.** The workspace compiles clean but emits ~200 warnings, dominated by `missing_docs` in `mempalace-storage` (~161), `mempalace-embeddings` (30), plus unused imports in `mempalace-ingest` and `mempalace-mcp` and a dead `fn run_cli` in `mempalace-cli`. All are `warn`-level today; promoting to `deny` would block CI until closed.
3. **Release binaries are offline by default, with an explicit bootstrap override.** `mempalace-cli` and `mempalace-mcp` now both honor `MEMPALACE_EMBED_ALLOW_DOWNLOADS` when constructing the fastembed provider. Without that env var they preserve the normal offline behavior; with it set to an explicit truthy value (`1`, `true`, or `yes`) they may download missing embedding assets into the local cache and show download progress on first run. This keeps model acquisition operator-controlled while giving both binaries the same documented first-run bootstrap path.
4. **`mempalace-rs-storage.yml` did not run on this branch.** The workflow is `paths:`-filtered to `mempalace-rs/**` and this branch only touches `docs/**`. The CI signal that normally covers workspace build and tests is therefore absent on the PR — any future validation-only branches should still run a manual dispatch or include a dummy change under `mempalace-rs/` to exercise CI.

## Reference-environment rows not covered here

Per [Packaging-And-Validation.md](Packaging-And-Validation.md), the following remain outstanding and must be captured in addition to this document:

- Successful GitHub Actions `build-and-package` output for the candidate revision.
- Artifact-based runtime rerun on this small VM using `mempalace-release-binaries`.
- Full low-CPU signoff against RSS and latency ceilings.
- Optional Python interop validation, applicable only if that feature ships.

## Release decision

This document covers only the small-VM runtime row of the validation matrix, and today it is still based on local builds. Per the release decision rule in `Packaging-And-Validation.md`, Rust v1 must not be marked release-ready from this document alone — attach the successful GitHub Actions packaging row and the artifact-based runtime rerun before cutting a release tag.
