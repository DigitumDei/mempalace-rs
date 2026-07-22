# Packaging And Validation

This document describes the release infrastructure: how binaries are built, signed, published as immutable candidates, promoted to stable, and verified by installers.

## Release Channels

MemPalace uses two distinct distribution channels:

| Channel | Tag pattern | Mutability | Default installer? | Description |
|---|---|---|---|---|
| **Stable** | `v<semver>` (e.g. `v0.1.0`) | Immutable | Yes | Signed, promoted from tested candidate |
| **Candidate** | `nightly-<40-hex-sha>` | Immutable | No | Built from every push to `main`; install explicitly for testing |

Both channels publish the same asset types and are signed with the same Ed25519 key.

## Release Artifacts

Each release builds `mempalace-cli` and `mempalace-mcp` for every supported platform.
Per-tool asset names:

- `*-linux-x86_64` — glibc, built on `ubuntu-latest`
- `*-macos-arm64` — Apple Silicon
- `*-windows-x86_64.exe`

The supported set is bounded by the prebuilt ONNX Runtime binaries that `ort`
ships (see `ort-sys`'s `dist.txt`). Targets `ort` does **not** provide a prebuilt
for are therefore not built:

- **musl** — no prebuilt (the original blocker; see the musl attempt in PR #12).
- **Intel macOS (`x86_64-apple-darwin`)** — `ort` 2.0.0-rc.11 ships no Intel-macOS
  binary at all.
- **Older glibc / "any container" (e.g. manylinux_2_28)** — `ort`'s Linux prebuilt
  requires **glibc 2.38+**, so the `linux-x86_64` binary's glibc floor is set by
  `ort`, not by the build host. Building on an older glibc baseline cannot lower it
  and fails to link (`undefined symbol: __isoc23_strtoull`). Broadening this would
  require compiling ONNX Runtime from source or switching to `ort-tract`.

Reference build command (host platforms):

```bash
cargo build --release --locked -p mempalace-cli -p mempalace-mcp --target <triple>
```

## Release Artifact Inventory

Every release ships these files per platform tag:

| File | Description |
|---|---|
| `mempalace-cli-<platform>` (or `.exe`) | CLI binary |
| `mempalace-mcp-<platform>` (or `.exe`) | MCP server binary |
| `manifest.json` | Structured release metadata (see below) |
| `manifest.json.sig` | Ed25519 signature over raw `manifest.json` bytes |
| `SHA256SUMS` | Per-file SHA-256 checksums |
| `SHA256SUMS.sig` | Ed25519 signature over raw `SHA256SUMS` bytes |

### manifest.json schema

```json
{
  "version": "0.1.0",
  "commit": "abc123def456...",
  "run_id": 1234567890,
  "run_attempt": 1,
  "tag": "v0.1.0",
  "channel": "stable",
  "assets": [
    {
      "name": "mempalace-cli-linux-x86_64",
      "sha256": "abcdef...",
      "size": 12345678
    }
  ]
}
```

Candidate manifests additionally include `candidate_tag` field linking to the
source nightly tag. Stable manifests reference the candidate they were promoted
from via the same field.

## Release Process

### 1. Candidate creation (automatic)

Triggered by every push to `main`. Defined in [ci.yml](../.github/workflows/ci.yml).

1. **Build and test** — all workspace tests, embedding baselines, CLI/MCP tests
2. **Release gate** — confirms all required jobs passed
3. **Release host** — builds `--release` binaries for each platform matrix leg
4. **Publish candidate** — collects artifacts, generates `manifest.json` and `SHA256SUMS`, signs both with the release signing key (stored as `RELEASE_SIGNING_KEY` GitHub secret), uploads everything as an immutable `nightly-<sha>` prerelease with build-provenance attestations
5. **No mutable rolling tag** — each commit produces a unique, never-overwritten release

### 2. Stable promotion (manual)

Triggered manually via the [Promote Release](../.github/workflows/promote-release.yml) workflow.

**Prerequisites:**

- The `release-promotion` GitHub environment is configured with required reviewers / approval
- The `RELEASE_SIGNING_KEY` secret is set in the repository or environment
- The `release/public-key.pem` file is committed to `main`

**Validation gates:**

1. Semantic version input (`v<major>.<minor>.<patch>`) must be valid and not already exist as a tag
2. Candidate tag must match `nightly-<40-hex-sha>` format
3. Candidate release must exist and be downloadable
4. `manifest.json.sig` and `SHA256SUMS.sig` must verify against the committed `release/public-key.pem`
5. Manifest commit must match the SHA embedded in the candidate tag
6. Manifest channel must be `nightly`
7. All assets listed in the manifest must exist on disk with matching checksums
8. At least 3 assets must be present (one CLI + MCP pair per platform, but all published platforms are verified)

**Promotion:**

1. The exact candidate binaries are copied (no rebuild)
2. A new stable `manifest.json` is created with `channel: "stable"` and `tag: "v<version>"`
3. New `SHA256SUMS` and signatures are generated
4. Build-provenance attestations are attached
5. The release is published as immutable `v<version>` (not a prerelease)
6. The stable tag cannot be overwritten — a tag that already exists will cause the workflow to fail

### 3. Verification by installer

The installers (`install.sh`, `install.ps1`) follow this trust chain:

```
Pinned public key (embedded in installer script)
  → verify manifest.json.sig
    → verify SHA256SUMS.sig
      → verify asset checksums against SHA256SUMS
        → install binaries
```

The public key is **never** fetched from the release server — it is pinned directly
in the installer source to prevent a compromise of the release infrastructure from
substituting the trust root.

## Signing Key Management

### Public key

- Committed as [`release/public-key.pem`](../release/public-key.pem)
- Embedded verbatim in both `install.sh` and `install.ps1`
- Ed25519 algorithm (OpenSSL-compatible)

### Private key

- Stored as the GitHub Actions secret `RELEASE_SIGNING_KEY`
- A single Ed25519 private key in PEM format
- Set at the repository level (or the `release-promotion` environment level for
  additional protection)
- Never committed to the repository, logged in CI output, or exposed in any way

### Generating a new keypair

```shell
# Generate
openssl genpkey -algorithm ed25519 -out release-private-key.pem

# Extract public key
openssl pkey -in release-private-key.pem -pubout > release/public-key.pem

# Encode private key for GitHub secret
openssl base64 -in release-private-key.pem | tr -d '\n'
# ^ set this as the RELEASE_SIGNING_KEY secret value
```

## Verification Commands (manual)

Verify the manifest signature:

```shell
openssl dgst -sha256 -verify release/public-key.pem \
  -signature manifest.json.sig manifest.json
```

Verify the checksums signature:

```shell
openssl dgst -sha256 -verify release/public-key.pem \
  -signature SHA256SUMS.sig SHA256SUMS
```

Verify asset integrity:

```shell
sha256sum --check SHA256SUMS --ignore-missing
```

## Reference CI Jobs

- GitHub Actions workflow: `.github/workflows/ci.yml`
- Jobs: `release-host` (matrix: linux glibc, macOS arm64, Windows) → `publish-candidate`
- Promotion workflow: `.github/workflows/promote-release.yml`
- Public key: `release/public-key.pem`
- Signing secret: `RELEASE_SIGNING_KEY` (GitHub Actions secret)

## Release Gate Rows

Rust v1 release signoff is split across two required rows:

### Row 1: Build and package on reference CI

Host:

- GitHub Actions `ubuntu-latest`, `macos-latest`, and `windows-latest` runners

Required outcomes:

- Workspace build passes.
- In-scope crate test jobs pass.
- Embedding baseline job passes.
- `release-host` (all matrix legs) completes.
- One `release-<asset>` artifact per platform is uploaded.
- `publish-candidate` creates an immutable `nightly-<sha>` prerelease with signed manifests and attestations.

This row is the source of truth for compilation, packaging, and the exact binaries promoted to runtime validation.

### Row 2: Runtime acceptance on the supported small VM

Host:

- The low-CPU VM that is intended to be supported in production.

Required outcomes:

- Install or unpack the exact `release-<asset>` artifact(s) built by Row 1 for the target platform.
- `mempalace-cli --help` succeeds.
- `init`, `mine`, `search`, `status`, and `wake-up` succeed against an isolated palace root.
- `mempalace-mcp` starts and responds successfully to MCP `initialize` plus `tools/list`.
- Low-CPU runtime expectations are recorded from this host, including degraded-behavior observations and any resource ceilings used for release signoff.

## Install Validation

Minimum install validation for a candidate release:

1. Download or copy the `release-<asset>` artifact for the target platform from the successful `release-host` run.
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
- release build for `mempalace-cli` and `mempalace-mcp` across all platform legs
- per-platform packaged artifact publication
- signed candidate prerelease with manifest and checksum signatures

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
- GitHub Actions `release-host` release matrix defined
- **Immutable candidate release pipeline with signed manifests**
- **Stable promotion workflow (manual, gated, no rebuild)**
- **Signature verification in installers (Ed25519, pinned trust root)**
- **Release public key committed and embedded in installer scripts**

Still environment-dependent:

- `RELEASE_SIGNING_KEY` secret configured in GitHub repository
- `release-promotion` GitHub environment created with required approvers
- successful `release-host` run on GitHub Actions for the candidate revision
- runtime acceptance pass on the supported small VM using the uploaded artifact
- full low-CPU signoff
- any optional Python interop validation

## Release Decision Rule

Do not mark Rust v1 release-ready from this document alone.

Use this directory to freeze the release promise, then attach both of the following before publishing a release tag:

- evidence from a successful GitHub Actions `release-host` run on the reference GitHub Actions hosts
- runtime acceptance evidence from the supported small VM using the exact uploaded release binaries
