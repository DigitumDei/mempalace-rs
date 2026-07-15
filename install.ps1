# MemPalace installer - downloads the Windows x86_64 nightly build, verifies
# checksums, installs to ~\.mempalace\bin, and registers the MCP server with
# detected AI tools.
#
#   irm https://raw.githubusercontent.com/DigitumDei/mempalace-rs/main/install.ps1 | iex
#
# Piped `iex` cannot pass parameters; either download the script first and run
# it with parameters, or set the env-var equivalents before the one-liner:
#   -NoSetup      / $env:MEMPALACE_NO_SETUP = '1'   skip MCP registration
#   -NoPath       / $env:MEMPALACE_NO_PATH  = '1'   skip PATH update
#   -InstallDir   / $env:MEMPALACE_INSTALL_DIR      install somewhere else

[CmdletBinding()]
param(
    [switch]$NoSetup,
    [switch]$NoPath,
    [string]$InstallDir
)

$ErrorActionPreference = 'Stop'

if (-not $NoSetup -and $env:MEMPALACE_NO_SETUP -eq '1') { $NoSetup = $true }
if (-not $NoPath -and $env:MEMPALACE_NO_PATH -eq '1') { $NoPath = $true }
if (-not $InstallDir) {
    if ($env:MEMPALACE_INSTALL_DIR) { $InstallDir = $env:MEMPALACE_INSTALL_DIR }
    else { $InstallDir = Join-Path $HOME '.mempalace\bin' }
}

$repo = 'DigitumDei/mempalace-rs'
$releaseUrl = "https://github.com/$repo/releases/download/nightly"
$assets = @('mempalace-cli-windows-x86_64.exe', 'mempalace-mcp-windows-x86_64.exe')

if ($env:PROCESSOR_ARCHITECTURE -ne 'AMD64' -and $env:PROCESSOR_ARCHITEW6432 -ne 'AMD64') {
    throw "Unsupported architecture: $env:PROCESSOR_ARCHITECTURE. Nightly builds cover Windows x86_64 only. Build from source instead: https://github.com/$repo/blob/main/docs/Quickstart.md"
}

$tmpDir = Join-Path ([System.IO.Path]::GetTempPath()) ("mempalace-install-" + [System.IO.Path]::GetRandomFileName())
New-Item -ItemType Directory -Path $tmpDir -Force | Out-Null

try {
    Write-Host 'Downloading MemPalace nightly (windows-x86_64)...'
    # Invoke-WebRequest is dramatically slower with the progress bar on PS 5.1.
    $prevProgress = $ProgressPreference
    $ProgressPreference = 'SilentlyContinue'
    try {
        foreach ($asset in $assets + 'SHA256SUMS') {
            Invoke-WebRequest -Uri "$releaseUrl/$asset" -OutFile (Join-Path $tmpDir $asset) -UseBasicParsing
        }
    } finally {
        $ProgressPreference = $prevProgress
    }

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
