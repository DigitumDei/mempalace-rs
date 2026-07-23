#!/usr/bin/env bash
set -euo pipefail

usage() {
    echo "usage: $0 <asset-dir> <expected-channel> <expected-tag> <expected-version> <trusted-public-key>" >&2
    exit 2
}

[ "$#" -eq 5 ] || usage

asset_dir="$1"
expected_channel="$2"
expected_tag="$3"
expected_version="$4"
public_key="$5"
manifest="$asset_dir/release-manifest.json"
checksums="$asset_dir/SHA256SUMS"

command -v jq >/dev/null 2>&1 || { echo "jq is required" >&2; exit 1; }
command -v openssl >/dev/null 2>&1 || { echo "openssl is required" >&2; exit 1; }
command -v sha256sum >/dev/null 2>&1 || { echo "sha256sum is required" >&2; exit 1; }

required_metadata=(
    SHA256SUMS
    SHA256SUMS.sig
    release-manifest.json
    release-manifest.sig
)
expected_assets=(
    mempalace-cli-linux-x86_64
    mempalace-mcp-linux-x86_64
    mempalace-cli-macos-arm64
    mempalace-mcp-macos-arm64
    mempalace-cli-windows-x86_64.exe
    mempalace-mcp-windows-x86_64.exe
)

for file in "${required_metadata[@]}" "${expected_assets[@]}"; do
    [ -f "$asset_dir/$file" ] || { echo "release bundle is missing: $file" >&2; exit 1; }
done
[ -f "$public_key" ] || { echo "trusted public key is missing: $public_key" >&2; exit 1; }

actual_files="$(
    for path in "$asset_dir"/*; do
        [ -f "$path" ] && basename "$path"
    done | LC_ALL=C sort
)"
expected_files="$(
    printf '%s\n' "${required_metadata[@]}" "${expected_assets[@]}" | LC_ALL=C sort
)"
[ "$actual_files" = "$expected_files" ] \
    || { echo "release bundle contains missing or unexpected files" >&2; exit 1; }

openssl dgst -sha256 -verify "$public_key" \
    -signature "$asset_dir/release-manifest.sig" "$manifest" >/dev/null
openssl dgst -sha256 -verify "$public_key" \
    -signature "$asset_dir/SHA256SUMS.sig" "$checksums" >/dev/null

expected_names_json="$(printf '%s\n' "${expected_assets[@]}" | jq -R . | jq -s .)"
jq -e \
    --arg channel "$expected_channel" \
    --arg tag "$expected_tag" \
    --arg version "$expected_version" \
    --argjson expected_names "$expected_names_json" \
    '
      .schema_version == 2
      and .channel == $channel
      and .release_tag == $tag
      and .version == $version
      and (.commit_sha | type == "string" and test("^[0-9a-f]{40}$"))
      and (.run_id | type == "string" and test("^[0-9]+$"))
      and (.checksums_sha256 | type == "string" and test("^[0-9a-f]{64}$"))
      and (.assets | type == "array" and length == 6)
      and ((.assets | map(.name) | sort) == ($expected_names | sort))
      and (all(.assets[];
          (.name | type == "string")
          and (.component == "cli" or .component == "mcp")
          and (.target | type == "string")
          and (.sha256 | type == "string" and test("^[0-9a-f]{64}$"))
          and (.size | type == "number" and . > 0)))
    ' "$manifest" >/dev/null

commit_sha="$(jq -r '.commit_sha' "$manifest")"
source_candidate_tag="$(jq -r '.source_candidate_tag // empty' "$manifest")"
case "$expected_channel" in
    nightly)
        [ "$expected_tag" = "v${expected_version}-nightly.${commit_sha}" ] \
            || { echo "candidate tag is not bound to the manifest version and commit" >&2; exit 1; }
        [ -z "$source_candidate_tag" ] \
            || { echo "candidate manifest must not have a source candidate tag" >&2; exit 1; }
        ;;
    stable)
        [ "$expected_tag" = "v$expected_version" ] \
            || { echo "stable tag is not bound to the manifest version" >&2; exit 1; }
        versioned_source_tag="v${expected_version}-nightly.${commit_sha}"
        legacy_source_tag="nightly-${commit_sha}"
        [ "$source_candidate_tag" = "$versioned_source_tag" ] \
            || {
                [ "$expected_version" = "0.1.0" ] \
                    && [ "$source_candidate_tag" = "$legacy_source_tag" ]
            } \
            || { echo "stable manifest is not bound to its source candidate" >&2; exit 1; }
        ;;
    *)
        echo "expected channel must be nightly or stable" >&2
        exit 1
        ;;
esac

actual_checksums_sha256="$(sha256sum "$checksums" | cut -d' ' -f1)"
manifest_checksums_sha256="$(jq -r '.checksums_sha256' "$manifest")"
[ "$actual_checksums_sha256" = "$manifest_checksums_sha256" ] \
    || { echo "signed manifest is not bound to SHA256SUMS" >&2; exit 1; }

checksum_names="$(awk '{name=$2; sub(/^\*/, "", name); print name}' "$checksums" | LC_ALL=C sort)"
expected_checksum_names="$(printf '%s\n' "${expected_assets[@]}" | LC_ALL=C sort)"
[ "$checksum_names" = "$expected_checksum_names" ] \
    || { echo "SHA256SUMS contains missing or unexpected assets" >&2; exit 1; }
(cd "$asset_dir" && sha256sum --strict --check SHA256SUMS >/dev/null)

for asset in "${expected_assets[@]}"; do
    expected_component="${asset#mempalace-}"
    expected_component="${expected_component%%-*}"
    expected_target="${asset#mempalace-cli-}"
    expected_target="${expected_target#mempalace-mcp-}"
    expected_target="${expected_target%.exe}"
    actual_sha256="$(sha256sum "$asset_dir/$asset" | cut -d' ' -f1)"
    actual_size="$(wc -c < "$asset_dir/$asset" | tr -d '[:space:]')"

    jq -e \
        --arg name "$asset" \
        --arg component "$expected_component" \
        --arg target "$expected_target" \
        --arg sha256 "$actual_sha256" \
        --argjson size "$actual_size" \
        '[.assets[] | select(
            .name == $name
            and .component == $component
            and .target == $target
            and .sha256 == $sha256
            and .size == $size
        )] | length == 1' "$manifest" >/dev/null \
        || { echo "manifest metadata does not match asset: $asset" >&2; exit 1; }
done
