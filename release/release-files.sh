#!/usr/bin/env bash
set -euo pipefail

[ "$#" -eq 1 ] || { echo "usage: $0 <asset-dir>" >&2; exit 2; }
asset_dir="$1"

files=(
    "$asset_dir/mempalace-cli-linux-x86_64"
    "$asset_dir/mempalace-mcp-linux-x86_64"
    "$asset_dir/mempalace-cli-macos-arm64"
    "$asset_dir/mempalace-mcp-macos-arm64"
    "$asset_dir/mempalace-cli-windows-x86_64.exe"
    "$asset_dir/mempalace-mcp-windows-x86_64.exe"
    "$asset_dir/SHA256SUMS"
    "$asset_dir/SHA256SUMS.sig"
    "$asset_dir/release-manifest.json"
    "$asset_dir/release-manifest.sig"
)

for file in "${files[@]}"; do
    [ -f "$file" ] || { echo "release asset is missing: $file" >&2; exit 1; }
    printf '%s\n' "$file"
done
