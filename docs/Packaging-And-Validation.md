# Packaging And Validation

This document captures the practical release path for the current Rust workspace and separates build/package signoff from runtime acceptance on the supported low-CPU VM.

## Release Artifacts

Current artifact set:

- `mempalace-cli` release binary
- `mempalace-mcp` release binary

Reference build command:

```bash
cargo build --release --locked -p mempalace-cli -p mempalace-mcp
```

Artifact paths:

- `mempalace-rs/target/release/mempalace-cli`
- `mempalace-rs/target/release/mempalace-mcp`

Reference packaging job:

- GitHub Actions workflow: `.github/workflows/mempalace-rs-storage.yml`
- Job: `build-and-package`
- Host: GitHub Actions `ubuntu-latest` runner
- Published artifact: `mempalace-release-binaries`

## Release Gate Rows

Rust v1 release signoff is split across two required rows:

### Row 1: Build and package on reference CI

Host:

- GitHub Actions `ubuntu-latest` runner

Required outcomes:

- Workspace build passes.
- In-scope crate test jobs pass.
- Embedding baseline job passes.
- `build-and-package` completes.
- `mempalace-release-binaries` artifact is uploaded.

This row is the source of truth for compilation, packaging, and the exact binaries promoted to runtime validation.

### Row 2: Runtime acceptance on the supported small VM

Host:

- The low-CPU VM that is intended to be supported in production.

Required outcomes:

- Install or unpack the exact `mempalace-release-binaries` artifact built by Row 1.
- `mempalace-cli --help` succeeds.
- `init`, `mine`, `search`, `status`, and `wake-up` succeed against an isolated palace root.
- `mempalace-mcp` starts and responds successfully to MCP `initialize` plus `tools/list`.
- Low-CPU runtime expectations are recorded from this host, including degraded-behavior observations and any resource ceilings used for release signoff.

## Install Validation

Minimum install validation for a candidate release:

1. Download or copy the `mempalace-release-binaries` artifact from the successful `build-and-package` run.
2. Run `mempalace-cli --help`.
3. Run `mempalace-cli init <fixture-dir>`.
4. Run `mempalace-cli mine <fixture-dir>`.
5. Run `mempalace-cli search <query>`.
6. Run `mempalace-cli status`.
7. Run `mempalace-cli wake-up`.
8. Start `mempalace-mcp` and confirm MCP `initialize` plus `tools/list`.

## Validation Matrix

### Expected on GitHub Actions packaging host

- workspace build
- per-crate unit and integration test jobs
- embedding baseline capture
- release build for `mempalace-cli` and `mempalace-mcp`
- packaged artifact publication

### Expected on the supported small VM

- release-artifact install-flow checks
- runtime smoke for CLI and MCP
- low-CPU suite
- final signoff on warm-cache behavior and resource ceilings
- optional Python interop validation only if that feature is explicitly shipped

## Current Phase 12 Status

Completed in this branch:

- CLI surface freeze documented
- config schema freeze documented
- release scope and known limitations documented
- standard deployment operator guidance written
- low-CPU operator guidance written
- packaging artifact definition documented
- GitHub Actions `build-and-package` release job defined

Still environment-dependent:

- successful `build-and-package` run on GitHub Actions for the candidate revision
- runtime acceptance pass on the supported small VM using the uploaded artifact
- full low-CPU signoff
- any optional Python interop validation

## Release Decision Rule

Do not mark Rust v1 release-ready from this document alone.

Use this directory to freeze the release promise, then attach both of the following before publishing a release tag:

- evidence from a successful GitHub Actions `build-and-package` run on the reference GitHub Actions host
- runtime acceptance evidence from the supported small VM using the exact uploaded release binaries
