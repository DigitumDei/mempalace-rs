#!/usr/bin/env bash
set -euo pipefail

usage() {
    echo "usage: $0 <asset-dir> <trusted-public-key>" >&2
    echo "RELEASE_SIGNING_KEY must contain the base64-encoded matching PEM private key." >&2
    exit 2
}

[ "$#" -eq 2 ] || usage

asset_dir="$1"
public_key="$2"

[ -f "$asset_dir/release-manifest.json" ] || { echo "release manifest is missing" >&2; exit 1; }
[ -f "$asset_dir/SHA256SUMS" ] || { echo "SHA256SUMS is missing" >&2; exit 1; }
[ -f "$public_key" ] || { echo "trusted public key is missing: $public_key" >&2; exit 1; }
[ -n "${RELEASE_SIGNING_KEY:-}" ] \
    || { echo "RELEASE_SIGNING_KEY is required" >&2; exit 1; }

private_key="$(mktemp)"
derived_public_key="$(mktemp)"
trusted_public_der="$(mktemp)"
derived_public_der="$(mktemp)"
cleanup() {
    rm -f "$private_key" "$derived_public_key" "$trusted_public_der" "$derived_public_der"
}
trap cleanup EXIT

chmod 600 "$private_key"
printf '%s' "$RELEASE_SIGNING_KEY" | base64 --decode > "$private_key"
openssl pkey -in "$private_key" -check -noout >/dev/null
openssl pkey -in "$private_key" -pubout -out "$derived_public_key" >/dev/null
openssl pkey -pubin -in "$public_key" -outform DER -out "$trusted_public_der"
openssl pkey -pubin -in "$derived_public_key" -outform DER -out "$derived_public_der"
cmp -s "$trusted_public_der" "$derived_public_der" \
    || { echo "release signing key does not match the committed public key" >&2; exit 1; }

openssl dgst -sha256 -sign "$private_key" \
    -out "$asset_dir/release-manifest.sig" "$asset_dir/release-manifest.json"
openssl dgst -sha256 -sign "$private_key" \
    -out "$asset_dir/SHA256SUMS.sig" "$asset_dir/SHA256SUMS"
