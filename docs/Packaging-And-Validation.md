# Packaging And Validation

This document captures the practical release path for the current Rust workspace and separates build/package signoff from runtime acceptance on the supported low-CPU VM.

## Release Artifacts

Each release ships one `agentpalace` executable with ONNX Runtime statically linked
into it, plus its license notices, per supported platform:

| Platform | Executable asset |
|---|---|
| Linux x86_64 | `agentpalace-linux-x86_64` |
| macOS arm64 | `agentpalace-macos-arm64` |
| Windows x86_64 | `agentpalace-windows-x86_64.exe` |

The build downloads the prebuilt runtime via `ort-sys`; no runtime DLL, shared
library, or runtime-path environment setting is required after deployment.
Executable and notices assets are covered by the signed release manifest and checksums.
Model weights remain separate cached assets. Linux uses glibc and is built on
`ubuntu-latest`; older Linux distributions, musl, and Intel macOS are outside the
current tested release matrix.

Reference build command (host platforms):

```bash
cargo build --release --locked -p agentpalace-cli --target <triple>
```

Reference packaging job:

- GitHub Actions workflow: `.github/workflows/ci.yml`
- Job: `release-host` (matrix: linux glibc, macOS arm64, Windows)
- Published artifacts: `release-<asset>` (e.g. `release-linux-x86_64`,
  `release-macos-arm64`), promoted into an immutable
  `v<version>-nightly.<full-commit-sha>`
  prerelease with signed `SHA256SUMS` and `release-manifest.json`, and GitHub
  build provenance attestations

## Release Gate Rows

Rust v1 release signoff is split across two required rows:

### Row 1: Build and package on reference CI

Host:

- GitHub Actions `ubuntu-latest`, `macos-15`, and `windows-latest` runners (the
  `release-host` matrix in [`.github/workflows/ci.yml`](../.github/workflows/ci.yml))

Required outcomes:

- `workspace-build` passes (`cargo check --workspace --all-targets --locked`).
- `clippy` passes (`cargo clippy --workspace --all-targets --locked`). The workspace denies
  `unwrap_used` among others, so this is a build-breaking gate, not advisory.
- Every per-package test job passes. `release-gate` aggregates them and is the single
  required predecessor of the release builds.
- `embedding-baselines` passes.
- `release-contract-tests` passes (shell/PowerShell installer syntax plus the signed
  release contract).
- `release-host` (all matrix legs) completes.
- One `release-<asset>` artifact per platform is uploaded, with a build-provenance
  attestation.

This row is the source of truth for compilation, packaging, and the exact binaries promoted to runtime validation.

### Row 2: Runtime acceptance on the supported small VM

Host:

- The low-CPU VM that is intended to be supported in production.

Required outcomes:

- Install or unpack the exact `release-<asset>` artifact(s) built by Row 1 for the target platform.
- `agentpalace --help` succeeds.
- `init`, `mine`, `search`, `status`, and `wake-up` succeed against an isolated palace root.
- `agentpalace serve --stdio` starts and responds successfully to MCP `initialize` plus `tools/list`.
- Low-CPU runtime expectations are recorded from this host, including degraded-behavior observations and any resource ceilings used for release signoff.

## Install Validation

Minimum install validation for a candidate release:

1. Download or copy the `release-<asset>` artifact for the target platform from the successful `release-host` run.
2. Run `agentpalace --help`.
3. Run `agentpalace init <fixture-dir>`.
4. Run `agentpalace mine <fixture-dir>`.
5. Run `agentpalace search <query>`.
6. Run `agentpalace status`.
7. Run `agentpalace wake-up`.
8. Start `agentpalace serve --stdio` and confirm MCP `initialize` plus `tools/list`.

## Validation Matrix

### Expected on GitHub Actions packaging host

- workspace build
- per-crate unit and integration test jobs
- embedding baseline capture
- release build for `agentpalace` with statically linked ONNX Runtime across all platform legs
- per-platform packaged artifact publication

### Expected on the supported small VM

- release-artifact install-flow checks
- runtime smoke for CLI and MCP
- low-CPU suite
- final signoff on warm-cache behavior and resource ceilings
- optional Python interop validation only if that feature is explicitly shipped

## Status

Documented and automated:

- CLI surface, config schema, release scope, and operator guidance are documented in this
  directory
- the GitHub Actions `release-host` release matrix is defined and gated behind `release-gate`
- candidate publication is automated: signed schema-v2 manifest, `SHA256SUMS`, immutable
  `v<version>-nightly.<full-commit-sha>` prerelease, and release attestation verification
  (see [Release Operations](Release-Operations.md))

Still environment-dependent, and required per-candidate:

- runtime acceptance pass on the supported small VM using the uploaded artifact
- full low-CPU signoff

## Release Decision Rule

Do not mark Rust v1 release-ready from this document alone.

Use this directory to freeze the release promise, then attach both of the following before publishing a release tag:

- evidence from a successful GitHub Actions `release-host` run on the reference GitHub Actions hosts
- runtime acceptance evidence from the supported small VM using the exact uploaded release binaries

## Promotion

After both rows pass, dispatch `.github/workflows/promote-release.yml` from the
protected `main` branch with the exact candidate tag. The stable semantic
version is taken from the candidate's verified signed manifest.
The workflow is bound to the `stable-release` protected environment and copies
the tested binaries without rebuilding them. Before promotion it verifies the
GitHub immutable-release attestation, every release asset, project signatures,
the tag and commit relationship, schema-v2 manifest metadata, the checksum-file
binding, and the original CI build provenance.

Repository administrators must complete the one-time controls in
[Release Operations](Release-Operations.md): enable GitHub immutable releases,
configure a required reviewer and protected-branch policy on `stable-release`,
and store the base64-encoded private key matching `release/public-key.pem` in
the `AGENTPALACE_RELEASE_SIGNING_KEY` environment secret. Candidate signing uses
the same protected environment so the private key is never exposed without
release approval.

Each platform also ships a signed `agentpalace-notices-<platform>.txt` asset containing
the upstream ONNX Runtime license and third-party notices, installed as `ONNXRuntime-NOTICES.txt`.

`release/ONNXRuntime-NOTICES.txt` contains the upstream ONNX Runtime 1.23.2 license
and third-party notices. Refresh it when upgrading the bundled runtime.
