# MemPalace installer - downloads the latest **stable** Windows x86_64 release,
# verifies the signed manifest and checksums using the committed public key,
# installs to ~\.mempalace\bin, and registers the MCP server with detected AI
# tools.
#
# Usage (stable, default):
#   irm https://raw.githubusercontent.com/DigitumDei/mempalace-rs/main/install.ps1 | iex
#
# Usage (immutable candidate / nightly, explicit tag):
#   $env:MEMPALACE_CHANNEL = 'nightly-<40-hex-commit-sha>'
#   irm https://raw.githubusercontent.com/DigitumDei/mempalace-rs/main/install.ps1 | iex
#
# Piped `iex` cannot pass parameters; either download the script first and run
# it with parameters, or set the env-var equivalents before the one-liner:
#   -Channel      / $env:MEMPALACE_CHANNEL         install channel (stable or explicit tag)
#   -NoSetup      / $env:MEMPALACE_NO_SETUP = '1'   skip MCP registration
#   -NoPath       / $env:MEMPALACE_NO_PATH  = '1'   skip PATH update
#   -InstallDir   / $env:MEMPALACE_INSTALL_DIR      install somewhere else

[CmdletBinding()]
param(
    [string]$Channel,
    [switch]$NoSetup,
    [switch]$NoPath,
    [string]$InstallDir
)

$ErrorActionPreference = 'Stop'

if (-not $Channel -and $env:MEMPALACE_CHANNEL) { $Channel = $env:MEMPALACE_CHANNEL }
if (-not $NoSetup -and $env:MEMPALACE_NO_SETUP -eq '1') { $NoSetup = $true }
if (-not $NoPath -and $env:MEMPALACE_NO_PATH -eq '1') { $NoPath = $true }
if (-not $InstallDir) {
    if ($env:MEMPALACE_INSTALL_DIR) { $InstallDir = $env:MEMPALACE_INSTALL_DIR }
    else { $InstallDir = Join-Path $HOME '.mempalace\bin' }
}

$repo = 'DigitumDei/mempalace-rs'
$assets = @('mempalace-cli-windows-x86_64.exe', 'mempalace-mcp-windows-x86_64.exe')

# ---------------------------------------------------------------------------
# Public key — pinned in the installer source. This is the Ed25519 public key
# whose corresponding private key signs all release manifests and checksum
# files. Never fetched from the release server.
# ---------------------------------------------------------------------------
$publicKeyPem = @'
-----BEGIN PUBLIC KEY-----
MCowBQYDK2VwAyEAXFaJde6SWshP25EyDG28lInqtXNRrW0fU4fbDyM/AQA=
-----END PUBLIC KEY-----
'@

if ($env:PROCESSOR_ARCHITECTURE -ne 'AMD64' -and $env:PROCESSOR_ARCHITEW6432 -ne 'AMD64') {
    throw "Unsupported architecture: $env:PROCESSOR_ARCHITECTURE. Supported: Windows x86_64 only. Build from source instead: https://github.com/$repo/blob/main/docs/Quickstart.md"
}

# ---------------------------------------------------------------------------
# Resolve channel — stable release tag or explicit candidate tag
# ---------------------------------------------------------------------------
if (-not $Channel -or $Channel -eq 'stable') {
    Write-Host 'Resolving latest stable release...'
    try {
        $apiUrl = "https://api.github.com/repos/$repo/releases/latest"
        $releaseData = Invoke-RestMethod -Uri $apiUrl -UseBasicParsing -ErrorAction Stop
        $releaseTag = $releaseData.tag_name
    } catch {
        throw "Could not determine latest stable release tag: $_"
    }
    if (-not $releaseTag) { throw 'Could not determine latest stable release tag' }
    Write-Host "Resolved latest stable release: $releaseTag"
    $Channel = 'stable'
} else {
    # Validate channel is an immutable tag
    if ($Channel -notmatch '^v[0-9]+\.[0-9]+\.[0-9]+$' -and $Channel -notmatch '^nightly-[0-9a-f]{40}$') {
        throw "Invalid channel '$Channel': must be 'stable' or an explicit tag like 'nightly-<40-hex-sha>'"
    }
}

$releaseTag = if ($Channel -eq 'stable') { $releaseTag } else { $Channel }
$releaseUrl = "https://github.com/$repo/releases/download/$releaseTag"

Write-Host "Channel: $Channel -> tag $releaseTag"

$tmpDir = Join-Path ([System.IO.Path]::GetTempPath()) ("mempalace-install-" + [System.IO.Path]::GetRandomFileName())
New-Item -ItemType Directory -Path $tmpDir -Force | Out-Null

try {
    Write-Host "Downloading MemPalace $releaseTag (windows-x86_64)..."
    # Invoke-WebRequest is dramatically slower with the progress bar on PS 5.1.
    $prevProgress = $ProgressPreference
    $ProgressPreference = 'SilentlyContinue'
    try {
        $downloadFiles = @('manifest.json', 'manifest.json.sig', 'SHA256SUMS', 'SHA256SUMS.sig') + $assets
        foreach ($asset in $downloadFiles) {
            Invoke-WebRequest -Uri "$releaseUrl/$asset" -OutFile (Join-Path $tmpDir $asset) -UseBasicParsing
        }
    } finally {
        $ProgressPreference = $prevProgress
    }

    # -----------------------------------------------------------------------
    # Signature verification — verify signatures over raw downloaded bytes.
    # Both manifest.json and SHA256SUMS are signed with the release private
    # key; we verify using the pinned public key before trusting any content.
    # -----------------------------------------------------------------------
    Write-Host 'Verifying signatures...'

    # Import the public key into an ECDsa object for Ed25519 verification
    function Verify-Ed25519Signature {
        param(
            [string]$DataPath,
            [string]$SigPath,
            [string]$Label
        )
        if (-not (Test-Path $DataPath)) { throw "Missing file for signature verification: $DataPath" }
        if (-not (Test-Path $SigPath)) { throw "Missing signature file: $SigPath" }

        # Read raw bytes
        $dataBytes = [System.IO.File]::ReadAllBytes((Resolve-Path $DataPath))
        $sigBytes = [System.IO.File]::ReadAllBytes((Resolve-Path $SigPath))

        # Parse the PEM public key to extract the raw key bytes
        $pemLines = $publicKeyPem -split "`n"
        $b64Key = ''
        foreach ($line in $pemLines) {
            $line = $line.Trim()
            if ($line -notlike '-----*' -and $line -ne '') {
                $b64Key += $line
            }
        }
        $keyBytes = [Convert]::FromBase64String($b64Key)

        # Ed25519 key format: 32-byte key follows algorithm identifier
        # PKCS#8 format: 32 bytes starting at offset 12 (or 16 for some encodings)
        # For the simple SPKI format: algorithm OID (2+2+6=10 bytes) + 0x03 + 0x21 + 0x00 + 32 bytes
        # Let's extract the last 32 bytes which is the raw key
        if ($keyBytes.Length -ge 32) {
            $rawKey = $keyBytes[-32..-1]
        } else {
            throw "Could not parse public key from PEM"
        }

        # .NET 7+ supports Ed25519 via ECDsa on Windows, or via specific APIs
        # Try using ECDsa with Ed25519 curve
        try {
            $ecdsa = [System.Security.Cryptography.ECDsa]::Create()
            # Import the key parameters
            $params = New-Object System.Security.Cryptography.ECParameters
            $params.Curve = [System.Security.Cryptography.ECCurve]::CreateFromFriendlyName('curve25519')
            $params.Q = New-Object System.Security.Cryptography.ECPoint
            $params.Q.X = $rawKey
            $params.Q.Y = @(0x00)  # Dummy Y - not used by Ed25519

            # Ed25519 uses different import. Try the new .NET 7+ approach
            $ecdsa.ImportSubjectPublicKeyInfo($keyBytes, [ref]$null)
            $result = $ecdsa.VerifyData($dataBytes, $sigBytes, [System.Security.Cryptography.HashAlgorithmName]::SHA256)
            if (-not $result) { throw "$Label signature verification FAILED" }
        } catch {
            # Fallback: try ImportEd25519 if available (.NET 8+)
            try {
                $ecdsa.Dispose()
                $ed = [System.Security.Cryptography.ECDsa]::Create()
                $ed.ImportSubjectPublicKeyInfo($keyBytes, [ref]$null)
                $result = $ed.VerifyData($dataBytes, $sigBytes, [System.Security.Cryptography.HashAlgorithmName]::SHA256)
                if (-not $result) { throw "$Label signature verification FAILED" }
            } catch {
                # Last resort: if openssl is available (e.g. via Git for Windows)
                $pubKeyFile = Join-Path $tmpDir 'verify-key.pem'
                [System.IO.File]::WriteAllText($pubKeyFile, $publicKeyPem, [System.Text.Encoding]::ASCII)
                $sigFile = (Resolve-Path $SigPath).Path
                $dataFile = (Resolve-Path $DataPath).Path
                $opensslResult = & openssl dgst -sha256 -verify $pubKeyFile -signature $sigFile $dataFile 2>&1
                if ($LASTEXITCODE -ne 0) { throw "$Label signature verification FAILED: $opensslResult" }
            }
        } finally {
            if ($ecdsa) { $ecdsa.Dispose() }
        }
        Write-Host "  $Label signature verified"
    }

    Verify-Ed25519Signature -DataPath (Join-Path $tmpDir 'manifest.json') `
                            -SigPath (Join-Path $tmpDir 'manifest.json.sig') `
                            -Label 'manifest.json'
    Verify-Ed25519Signature -DataPath (Join-Path $tmpDir 'SHA256SUMS') `
                            -SigPath (Join-Path $tmpDir 'SHA256SUMS.sig') `
                            -Label 'SHA256SUMS'

    # -----------------------------------------------------------------------
    # Validate manifest channel matches installation channel
    # -----------------------------------------------------------------------
    Write-Host 'Validating manifest metadata...'
    $manifestJson = Get-Content (Join-Path $tmpDir 'manifest.json') -Raw | ConvertFrom-Json
    $manifestChannel = $manifestJson.channel
    $manifestTag = $manifestJson.tag

    if ($Channel -eq 'stable' -and $manifestChannel -ne 'stable') {
        throw "Manifest channel is '$manifestChannel', expected 'stable' for default install"
    }
    if ($Channel -ne 'stable' -and $manifestTag -ne $releaseTag) {
        throw "Manifest tag '$manifestTag' does not match requested channel tag '$releaseTag'"
    }
    Write-Host "  Channel/version metadata validated (tag=$manifestTag, channel=$manifestChannel)"

    # -----------------------------------------------------------------------
    # Checksum verification — verify against the now-trusted SHA256SUMS
    # -----------------------------------------------------------------------
    Write-Host 'Verifying checksums...'
    $sums = @{}
    foreach ($line in Get-Content (Join-Path $tmpDir 'SHA256SUMS')) {
        if ($line -match '^([0-9a-fA-F]{64})\s+\*?(.+)$') {
            $sums[$Matches[2].Trim()] = $Matches[1].ToLowerInvariant()
        }
    }
    foreach ($asset in $assets) {
        if (-not $sums.ContainsKey($asset)) {
            throw "SHA256SUMS is missing an entry for $asset"
        }
        $actual = (Get-FileHash -Algorithm SHA256 -Path (Join-Path $tmpDir $asset)).Hash.ToLowerInvariant()
        if ($actual -ne $sums[$asset]) {
            throw "Checksum verification FAILED for $asset - aborting install"
        }
    }
    Write-Host '  Asset checksums verified'

    # -----------------------------------------------------------------------
    # Install
    # -----------------------------------------------------------------------
    $updated = Test-Path (Join-Path $InstallDir 'mempalace-cli.exe')
    New-Item -ItemType Directory -Path $InstallDir -Force | Out-Null

    # Clean up .old files left behind by a previous locked-file update.
    Get-ChildItem -Path $InstallDir -Filter '*.exe.old' -ErrorAction SilentlyContinue | ForEach-Object {
        try { Remove-Item $_.FullName -Force -Confirm:$false -ErrorAction Stop } catch {}
    }

    foreach ($asset in $assets) {
        # mempalace-cli-windows-x86_64.exe -> mempalace-cli.exe
        $target = Join-Path $InstallDir ($asset -replace '-windows-x86_64\.exe$', '.exe')
        $source = Join-Path $tmpDir $asset
        try {
            Move-Item -Path $source -Destination $target -Force -ErrorAction Stop
        } catch {
            # A running MCP server locks its exe; Windows still allows renaming
            # a running exe, so move the old one aside and retry.
            Move-Item -Path $target -Destination "$target.old" -Force
            Move-Item -Path $source -Destination $target -Force
        }
    }

    if ($updated) {
        Write-Host "Updated existing install in $InstallDir"
    } else {
        Write-Host "Installed mempalace-cli.exe and mempalace-mcp.exe to $InstallDir"
    }

    if (-not $NoPath) {
        $userPath = [Environment]::GetEnvironmentVariable('Path', 'User')
        $onPath = ($userPath -split ';') -contains $InstallDir
        if (-not $onPath) {
            $newPath = if ($userPath) { "$userPath;$InstallDir" } else { $InstallDir }
            [Environment]::SetEnvironmentVariable('Path', $newPath, 'User')
            Write-Host "Added $InstallDir to your user PATH - new terminals will pick it up."
        }
        if (($env:Path -split ';') -notcontains $InstallDir) {
            $env:Path = "$InstallDir;$env:Path"
        }
    }

    if (-not $NoSetup) {
        & (Join-Path $InstallDir 'mempalace-cli.exe') setup
    } else {
        Write-Host 'Skipped MCP registration. Run it later with:'
        Write-Host "  $(Join-Path $InstallDir 'mempalace-cli.exe') setup"
    }

    Write-Host ''
    Write-Host 'MemPalace is installed. Next steps:'
    Write-Host '  mempalace-cli init C:\path\to\your\project    # create a palace for a project'
    Write-Host '  mempalace-cli mine C:\path\to\your\project    # ingest its files'
    Write-Host ''
    Write-Host "Full walkthrough: https://github.com/$repo/blob/main/docs/Quickstart.md"
} finally {
    Remove-Item -Path $tmpDir -Recurse -Force -Confirm:$false -ErrorAction SilentlyContinue
}
