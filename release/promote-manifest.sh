#!/usr/bin/env bash
set -euo pipefail

usage() {
    echo "usage: $0 <asset-dir> <stable-version>" >&2
    exit 2
}

[ "$#" -eq 2 ] || usage

asset_dir="$1"
version="$2"
manifest="$asset_dir/release-manifest.json"

[ -f "$manifest" ] || { echo "release manifest is missing" >&2; exit 1; }
[[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?(\+[0-9A-Za-z.-]+)?$ ]] \
    || { echo "version must be semantic" >&2; exit 1; }

source_candidate_tag="$(jq -r '.release_tag' "$manifest")"
candidate_version="$(jq -r '.version' "$manifest")"
candidate_commit="$(jq -r '.commit_sha' "$manifest")"
[ "$candidate_version" = "$version" ] \
    || { echo "candidate version does not match requested stable version" >&2; exit 1; }
[[ "$candidate_commit" =~ ^[0-9a-f]{40}$ ]] \
    || { echo "candidate commit SHA is invalid" >&2; exit 1; }
[ "$source_candidate_tag" = "v${version}-nightly.${candidate_commit}" ] \
    || { echo "source manifest tag does not match its version and commit" >&2; exit 1; }

temporary_manifest="$asset_dir/release-manifest.stable.json"
jq \
    --arg version "$version" \
    --arg release_tag "v$version" \
    --arg source_candidate_tag "$source_candidate_tag" \
    '.channel = "stable"
     | .version = $version
     | .release_tag = $release_tag
     | .source_candidate_tag = $source_candidate_tag' \
    "$manifest" > "$temporary_manifest"
mv "$temporary_manifest" "$manifest"

rm -f "$asset_dir/release-manifest.sig" "$asset_dir/SHA256SUMS.sig"
