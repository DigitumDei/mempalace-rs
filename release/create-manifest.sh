#!/usr/bin/env bash
set -euo pipefail

usage() {
    echo "usage: $0 <asset-dir> <channel> <version> <commit-sha> <run-id> <release-tag>" >&2
    exit 2
}

[ "$#" -eq 6 ] || usage

asset_dir="$1"
channel="$2"
version="$3"
commit_sha="$4"
run_id="$5"
release_tag="$6"

[ -d "$asset_dir" ] || { echo "asset directory does not exist: $asset_dir" >&2; exit 1; }
command -v jq >/dev/null 2>&1 || { echo "jq is required" >&2; exit 1; }
command -v sha256sum >/dev/null 2>&1 || { echo "sha256sum is required" >&2; exit 1; }

[[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?(\+[0-9A-Za-z.-]+)?$ ]] \
    || { echo "version must be semantic" >&2; exit 1; }
[[ "$commit_sha" =~ ^[0-9a-f]{40}$ ]] \
    || { echo "commit SHA must be 40 lowercase hexadecimal characters" >&2; exit 1; }
[[ "$run_id" =~ ^[0-9]+$ ]] \
    || { echo "run ID must be numeric" >&2; exit 1; }

case "$channel" in
    nightly)
        [ "$release_tag" = "v${version}-nightly.${commit_sha}" ] \
            || { echo "nightly release tag must match the version and commit SHA" >&2; exit 1; }
        ;;
    stable)
        [ "$release_tag" = "v$version" ] \
            || { echo "stable release tag must match the version" >&2; exit 1; }
        ;;
    *)
        echo "channel must be nightly or stable" >&2
        exit 1
        ;;
esac

expected_assets=(
    mempalace-cli-linux-x86_64
    mempalace-mcp-linux-x86_64
    mempalace-cli-macos-arm64
    mempalace-mcp-macos-arm64
    mempalace-cli-windows-x86_64.exe
    mempalace-mcp-windows-x86_64.exe
)

for asset in "${expected_assets[@]}"; do
    [ -f "$asset_dir/$asset" ] || { echo "missing release asset: $asset" >&2; exit 1; }
done

actual_assets="$(
    for path in "$asset_dir"/mempalace-*; do
        [ -f "$path" ] && basename "$path"
    done | LC_ALL=C sort
)"
expected_asset_list="$(printf '%s\n' "${expected_assets[@]}" | LC_ALL=C sort)"
[ "$actual_assets" = "$expected_asset_list" ] \
    || { echo "release asset set contains missing or unexpected mempalace files" >&2; exit 1; }

(
    cd "$asset_dir"
    sha256sum "${expected_assets[@]}" > SHA256SUMS
)
checksums_sha256="$(sha256sum "$asset_dir/SHA256SUMS" | cut -d' ' -f1)"

assets_json="$(
    for asset in "${expected_assets[@]}"; do
        component="${asset#mempalace-}"
        component="${component%%-*}"
        target="${asset#mempalace-cli-}"
        target="${target#mempalace-mcp-}"
        target="${target%.exe}"
        sha256="$(sha256sum "$asset_dir/$asset" | cut -d' ' -f1)"
        size="$(wc -c < "$asset_dir/$asset" | tr -d '[:space:]')"
        jq -n \
            --arg name "$asset" \
            --arg component "$component" \
            --arg target "$target" \
            --arg sha256 "$sha256" \
            --argjson size "$size" \
            '{name: $name, component: $component, target: $target, sha256: $sha256, size: $size}'
    done | jq -s .
)"

jq -n \
    --arg channel "$channel" \
    --arg version "$version" \
    --arg commit_sha "$commit_sha" \
    --arg run_id "$run_id" \
    --arg release_tag "$release_tag" \
    --arg checksums_sha256 "$checksums_sha256" \
    --argjson assets "$assets_json" \
    '{
        schema_version: 2,
        channel: $channel,
        version: $version,
        commit_sha: $commit_sha,
        run_id: $run_id,
        release_tag: $release_tag,
        source_candidate_tag: null,
        checksums_sha256: $checksums_sha256,
        assets: $assets
    }' > "$asset_dir/release-manifest.json"
