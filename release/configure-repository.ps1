[CmdletBinding(SupportsShouldProcess)]
param(
    [string]$Repository = 'DigitumDei/mempalace-rs',
    [Parameter(Mandatory)]
    [string]$Reviewer,
    [Parameter(Mandatory)]
    [string]$PrivateKeyPath,
    [switch]$PreventSelfReview
)

$ErrorActionPreference = 'Stop'

if (-not (Get-Command gh -ErrorAction SilentlyContinue)) {
    throw 'GitHub CLI (gh) is required.'
}
if (-not (Get-Command openssl -ErrorAction SilentlyContinue)) {
    throw 'OpenSSL is required to validate the signing key.'
}
if (-not (Test-Path -LiteralPath $PrivateKeyPath -PathType Leaf)) {
    throw "Private key does not exist: $PrivateKeyPath"
}

$trustedPublicKey = Join-Path $PSScriptRoot 'public-key.pem'
$temporaryDirectory = Join-Path ([IO.Path]::GetTempPath()) ("mempalace-release-config-" + [IO.Path]::GetRandomFileName())
New-Item -ItemType Directory -Path $temporaryDirectory -Force | Out-Null

try {
    & gh auth status
    if ($LASTEXITCODE -ne 0) { throw 'GitHub CLI is not authenticated.' }

    $derivedPublicDer = Join-Path $temporaryDirectory 'derived-public.der'
    $trustedPublicDer = Join-Path $temporaryDirectory 'trusted-public.der'
    & openssl pkey -in $PrivateKeyPath -pubout -outform DER -out $derivedPublicDer
    if ($LASTEXITCODE -ne 0) { throw 'Could not derive the public key from the private key.' }
    & openssl pkey -pubin -in $trustedPublicKey -outform DER -out $trustedPublicDer
    if ($LASTEXITCODE -ne 0) { throw 'Could not read release/public-key.pem.' }

    $derivedFingerprint = (Get-FileHash -Algorithm SHA256 -LiteralPath $derivedPublicDer).Hash
    $trustedFingerprint = (Get-FileHash -Algorithm SHA256 -LiteralPath $trustedPublicDer).Hash
    if ($derivedFingerprint -ne $trustedFingerprint) {
        throw 'The supplied private key does not match release/public-key.pem.'
    }

    $reviewerId = & gh api "users/$Reviewer" --jq '.id'
    if ($LASTEXITCODE -ne 0 -or -not $reviewerId) {
        throw "Could not resolve GitHub reviewer: $Reviewer"
    }

    $environmentPayload = @{
        wait_timer = 0
        prevent_self_review = [bool]$PreventSelfReview
        reviewers = @(
            @{
                type = 'User'
                id = [int64]$reviewerId
            }
        )
        deployment_branch_policy = @{
            protected_branches = $true
            custom_branch_policies = $false
        }
    }
    $environmentPayloadPath = Join-Path $temporaryDirectory 'environment.json'
    $environmentPayload | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $environmentPayloadPath -Encoding utf8NoBOM

    if ($PSCmdlet.ShouldProcess($Repository, 'enable immutable releases and configure stable-release')) {
        & gh api `
            -X PUT `
            -H 'Accept: application/vnd.github+json' `
            -H 'X-GitHub-Api-Version: 2026-03-10' `
            "repos/$Repository/immutable-releases"
        if ($LASTEXITCODE -ne 0) { throw 'Could not enable GitHub immutable releases.' }

        & gh api `
            -X PUT `
            -H 'Accept: application/vnd.github+json' `
            -H 'X-GitHub-Api-Version: 2026-03-10' `
            "repos/$Repository/environments/stable-release" `
            --input $environmentPayloadPath
        if ($LASTEXITCODE -ne 0) { throw 'Could not configure the stable-release environment.' }

        $privateKeyBytes = [IO.File]::ReadAllBytes((Resolve-Path -LiteralPath $PrivateKeyPath))
        $encodedPrivateKey = [Convert]::ToBase64String($privateKeyBytes)
        $encodedPrivateKey | & gh secret set `
            MEMPALACE_RELEASE_SIGNING_KEY `
            --repo $Repository `
            --env stable-release
        if ($LASTEXITCODE -ne 0) { throw 'Could not configure the release signing secret.' }

        $immutableEnabled = & gh api `
            -H 'X-GitHub-Api-Version: 2026-03-10' `
            "repos/$Repository/immutable-releases" `
            --jq '.enabled'
        if ($LASTEXITCODE -ne 0 -or $immutableEnabled -ne 'true') {
            throw 'GitHub immutable releases are not enabled after configuration.'
        }

        $environment = (& gh api `
            -H 'X-GitHub-Api-Version: 2026-03-10' `
            "repos/$Repository/environments/stable-release") | ConvertFrom-Json
        if ($LASTEXITCODE -ne 0) { throw 'Could not verify the stable-release environment.' }
        if (-not ($environment.protection_rules | Where-Object type -eq 'required_reviewers')) {
            throw 'The stable-release environment has no required reviewer after configuration.'
        }
        if (-not $environment.deployment_branch_policy.protected_branches) {
            throw 'The stable-release environment is not restricted to protected branches.'
        }

        $secretList = & gh secret list --repo $Repository --env stable-release
        if ($LASTEXITCODE -ne 0 -or $secretList -notmatch '(?m)^MEMPALACE_RELEASE_SIGNING_KEY\s') {
            throw 'The release signing secret is not visible after configuration.'
        }

        & gh variable set `
            MEMPALACE_IMMUTABLE_RELEASES_ENABLED `
            --body true `
            --repo $Repository `
            --env stable-release
        if ($LASTEXITCODE -ne 0) {
            throw 'Could not record verified immutable-release provisioning.'
        }

        $immutableMarker = & gh variable get `
            MEMPALACE_IMMUTABLE_RELEASES_ENABLED `
            --repo $Repository `
            --env stable-release
        if ($LASTEXITCODE -ne 0 -or $immutableMarker -ne 'true') {
            throw 'The immutable-release provisioning marker is not visible after configuration.'
        }
    }

    $resultVerb = if ($WhatIfPreference) { 'validated locally for' } else { 'configured for' }
    Write-Host "Release policy $resultVerb $Repository."
    Write-Host "Trusted public-key fingerprint: $trustedFingerprint"
    Write-Host "Required reviewer: $Reviewer"
    Write-Host "Prevent self-review: $([bool]$PreventSelfReview)"
    if (-not $WhatIfPreference) {
        Write-Host 'Immutable release marker: true'
    }
} finally {
    Remove-Item -LiteralPath $temporaryDirectory -Recurse -Force -ErrorAction SilentlyContinue
}
