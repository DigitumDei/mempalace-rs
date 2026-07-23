# Release Operations

This runbook is the operator contract for signed candidate and stable releases.
The workflow fails closed when any required repository setting, signature,
artifact, or provenance record is missing.

## Trust model

- `main` remains protected and changes arrive through pull requests.
- GitHub immutable releases lock every published release tag and asset and
  produce a GitHub release attestation.
- `release/public-key.pem` is the installer trust root.
- The matching private key exists only as the
  `MEMPALACE_RELEASE_SIGNING_KEY` secret in the protected `stable-release`
  environment and in the operator's offline recovery store.
- The same protected environment gates candidate signing and stable promotion.
  This deliberately requires release approval before the private key is exposed
  to either job.
- Stable promotion downloads the immutable candidate rather than rebuilding.
  It verifies the GitHub release attestation, each release asset, the project
  signatures, the tag/commit relationship, manifest metadata, checksums, and
  build provenance from `.github/workflows/ci.yml`.

The signed manifest binds its channel, version, tag, commit, source candidate,
checksum-file digest, and every asset's name, component, target, digest, and
size. `SHA256SUMS` remains separately signed for defense in depth.

The one-line installers are bootstrap programs fetched from the protected
`main` branch over HTTPS. Their embedded key protects downloaded release
metadata and binaries; it does not make a modified installer trustworthy.
Operators should treat branch protection and independent review of installer
changes as part of the distribution trust boundary.

## One-time repository provisioning

Before merging or rerunning a release-producing workflow:

1. Retain the private key that matches `release/public-key.pem` in an offline
   recovery store. Never commit it.
2. Choose the GitHub user who must approve candidate signing and stable
   promotion.
3. Run the repository configuration helper from PowerShell:

   ```powershell
   ./release/configure-repository.ps1 `
     -Reviewer DigitumDei `
     -PrivateKeyPath C:\secure\mempalace-release-private.pem
   ```

   Add `-PreventSelfReview` only when a second authorized operator can approve
   releases initiated by the primary operator.

The helper:

- proves the private key matches the committed public key before sending
  anything to GitHub;
- enables GitHub immutable releases;
- restricts `stable-release` deployments to protected branches and adds the
  required reviewer;
- stores only the base64-encoded private key in the environment secret;
- records `MEMPALACE_IMMUTABLE_RELEASES_ENABLED=true` in the protected
  environment only after the live admin API confirms that immutability is on.

The workflow's short-lived `GITHUB_TOKEN` cannot read repository Administration
settings, so release jobs require that protected provisioning marker instead of
calling the admin-only API. Each published release is then checked through
ordinary release metadata. If GitHub reports a newly published release as
mutable, the workflow removes that release and its tag before failing. If the
metadata request itself is indeterminate, the workflow fails without deleting
anything so an operator can inspect the release.

Re-run with `-WhatIf` to validate the local key and reviewer without mutating
repository settings.

Repository owners can independently verify the live controls:

```powershell
gh api -H "X-GitHub-Api-Version: 2026-03-10" `
  repos/DigitumDei/mempalace-rs/immutable-releases
gh api repos/DigitumDei/mempalace-rs/environments/stable-release
gh secret list --repo DigitumDei/mempalace-rs --env stable-release
gh variable get MEMPALACE_IMMUTABLE_RELEASES_ENABLED `
  --repo DigitumDei/mempalace-rs --env stable-release
```

## Candidate publication

Every successful push to `main` builds and attests six binaries. After the
`stable-release` approval, CI:

1. creates schema-v2 `release-manifest.json` and `SHA256SUMS`;
2. checks that the environment key matches the committed public key;
3. signs and locally re-verifies both files;
4. publishes `nightly-<full-main-commit-sha>` as a GitHub immutable prerelease;
5. verifies the resulting GitHub release and every published asset.

A rerun reuses an already-published immutable candidate only after re-verifying
the release and project signatures. It never overwrites the release.

## Runtime signoff

Download the exact candidate named in the workflow and complete both release
gate rows in [Packaging and Validation](Packaging-And-Validation.md). Record:

- candidate tag and commit SHA;
- CI run URL;
- GitHub release verification result;
- supported small-VM runtime evidence;
- approver and timestamp.

Do not promote a different candidate, even if it has the same Cargo version.

## Stable promotion

Dispatch `promote-release` from `main` with:

- `candidate_tag`: the exact signed-off `nightly-<full-sha>` tag;
- `version`: the Cargo semantic version without `v`.

After environment approval, promotion verifies the candidate end to end,
rewrites only stable-channel manifest fields, re-signs the manifest, publishes
the exact allowlisted files as immutable `v<version>`, and verifies the
published release attestation.

The workflow refuses an existing stable release or tag. Immutable release names
must never be reused.

## First stable release

The normal installers intentionally use only GitHub's latest stable release.
Until the first stable release exists, they fail with a clear message and do
not fall back to the mutable legacy `nightly` tag.

Bootstrap in this order:

1. provision repository controls and the matching signing key;
2. merge the release workflow;
3. approve and verify the new immutable candidate;
4. complete runtime signoff;
5. promote `v0.1.0`;
6. run both documented installers against the published stable release;
7. announce stable installer availability.

## Key rotation

Key rotation is a coordinated release change:

1. generate and secure a new private key;
2. update `release/public-key.pem` and the embedded keys in both installers in
   the same pull request;
3. pass release contract tests and independent review;
4. update the protected environment secret only after that pull request merges;
5. publish and verify a new candidate before the next stable promotion.

Old installers continue to trust only the old key, so retain old stable
releases and plan compatibility explicitly.
