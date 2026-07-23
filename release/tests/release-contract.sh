#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
temporary_root="$(mktemp -d)"
cleanup() {
    rm -rf "$temporary_root"
}
trap cleanup EXIT

bash "$repo_root/release/tests/build-version.sh"

extract_embedded_key() {
    awk '
        /-----BEGIN PUBLIC KEY-----/ { capture = 1 }
        capture { sub(/\r$/, ""); print }
        /-----END PUBLIC KEY-----/ { exit }
    ' "$1"
}

trusted_key_text="$(tr -d '\r' < "$repo_root/release/public-key.pem")"
[ "$(extract_embedded_key "$repo_root/install.sh")" = "$trusted_key_text" ] \
    || { echo "install.sh embedded key differs from release/public-key.pem" >&2; exit 1; }
[ "$(extract_embedded_key "$repo_root/install.ps1")" = "$trusted_key_text" ] \
    || { echo "install.ps1 embedded key differs from release/public-key.pem" >&2; exit 1; }

private_key="$temporary_root/private-key.pem"
wrong_private_key="$temporary_root/wrong-private-key.pem"
public_key="$temporary_root/public-key.pem"

openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out "$private_key" 2>/dev/null
openssl pkey -in "$private_key" -pubout -out "$public_key" >/dev/null
openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out "$wrong_private_key" 2>/dev/null

encode_key() {
    base64 < "$1" | tr -d '\r\n'
}

make_assets() {
    local directory="$1"
    local marker="$2"
    mkdir -p "$directory"
    for name in \
        mempalace-cli-linux-x86_64 \
        mempalace-mcp-linux-x86_64 \
        mempalace-cli-macos-arm64 \
        mempalace-mcp-macos-arm64 \
        mempalace-cli-windows-x86_64.exe \
        mempalace-mcp-windows-x86_64.exe
    do
        printf '%s:%s\n' "$marker" "$name" > "$directory/$name"
    done
}

create_and_sign_candidate() {
    local directory="$1"
    local marker="$2"
    local commit_sha="$3"
    make_assets "$directory" "$marker"
    bash "$repo_root/release/create-manifest.sh" \
        "$directory" nightly 0.1.0 "$commit_sha" 12345 "v0.1.0-nightly.$commit_sha"
    RELEASE_SIGNING_KEY="$(encode_key "$private_key")" \
        bash "$repo_root/release/sign-manifest.sh" "$directory" "$public_key"
}

expect_failure() {
    local description="$1"
    shift
    if "$@" >"$temporary_root/expected-failure.log" 2>&1; then
        echo "expected failure: $description" >&2
        exit 1
    fi
}

commit_a="1111111111111111111111111111111111111111"
commit_b="2222222222222222222222222222222222222222"
candidate_a="$temporary_root/candidate-a"
candidate_b="$temporary_root/candidate-b"

create_and_sign_candidate "$candidate_a" "candidate-a" "$commit_a"
bash "$repo_root/release/verify-manifest.sh" \
    "$candidate_a" nightly "v0.1.0-nightly.$commit_a" 0.1.0 "$public_key"

tampered_binary="$temporary_root/tampered-binary"
cp -R "$candidate_a" "$tampered_binary"
printf 'tampered\n' >> "$tampered_binary/mempalace-cli-linux-x86_64"
expect_failure "tampered binary" \
    bash "$repo_root/release/verify-manifest.sh" \
    "$tampered_binary" nightly "v0.1.0-nightly.$commit_a" 0.1.0 "$public_key"

create_and_sign_candidate "$candidate_b" "candidate-b" "$commit_b"
hybrid_bundle="$temporary_root/hybrid-bundle"
cp -R "$candidate_b" "$hybrid_bundle"
cp "$candidate_a/release-manifest.json" "$hybrid_bundle/release-manifest.json"
cp "$candidate_a/release-manifest.sig" "$hybrid_bundle/release-manifest.sig"
expect_failure "manifest replayed with another signed checksum bundle" \
    bash "$repo_root/release/verify-manifest.sh" \
    "$hybrid_bundle" nightly "v0.1.0-nightly.$commit_a" 0.1.0 "$public_key"

expect_failure "nightly tag does not match manifest commit" \
    bash "$repo_root/release/verify-manifest.sh" \
    "$candidate_a" nightly "v0.1.0-nightly.$commit_b" 0.1.0 "$public_key"

expect_failure "nightly tag does not match manifest version" \
    bash "$repo_root/release/verify-manifest.sh" \
    "$candidate_a" nightly "v0.1.1-nightly.$commit_a" 0.1.0 "$public_key"

wrong_key_bundle="$temporary_root/wrong-key"
make_assets "$wrong_key_bundle" "wrong-key"
bash "$repo_root/release/create-manifest.sh" \
    "$wrong_key_bundle" nightly 0.1.0 "$commit_a" 12345 "v0.1.0-nightly.$commit_a"
expect_failure "private key does not match trusted public key" \
    env RELEASE_SIGNING_KEY="$(encode_key "$wrong_private_key")" \
    bash "$repo_root/release/sign-manifest.sh" "$wrong_key_bundle" "$public_key"

stable_bundle="$temporary_root/stable"
cp -R "$candidate_a" "$stable_bundle"
bash "$repo_root/release/promote-manifest.sh" "$stable_bundle" 0.1.0
RELEASE_SIGNING_KEY="$(encode_key "$private_key")" \
    bash "$repo_root/release/sign-manifest.sh" "$stable_bundle" "$public_key"
bash "$repo_root/release/verify-manifest.sh" \
    "$stable_bundle" stable v0.1.0 0.1.0 "$public_key"

legacy_stable="$temporary_root/legacy-stable"
cp -R "$stable_bundle" "$legacy_stable"
jq --arg source_candidate_tag "nightly-$commit_a" \
    '.source_candidate_tag = $source_candidate_tag' \
    "$legacy_stable/release-manifest.json" > "$legacy_stable/release-manifest.legacy.json"
mv "$legacy_stable/release-manifest.legacy.json" "$legacy_stable/release-manifest.json"
RELEASE_SIGNING_KEY="$(encode_key "$private_key")" \
    bash "$repo_root/release/sign-manifest.sh" "$legacy_stable" "$public_key"
bash "$repo_root/release/verify-manifest.sh" \
    "$legacy_stable" stable v0.1.0 0.1.0 "$public_key"

future_legacy_stable="$temporary_root/future-legacy-stable"
cp -R "$legacy_stable" "$future_legacy_stable"
jq \
    '.version = "0.2.0" | .release_tag = "v0.2.0"' \
    "$future_legacy_stable/release-manifest.json" \
    > "$future_legacy_stable/release-manifest.future.json"
mv \
    "$future_legacy_stable/release-manifest.future.json" \
    "$future_legacy_stable/release-manifest.json"
RELEASE_SIGNING_KEY="$(encode_key "$private_key")" \
    bash "$repo_root/release/sign-manifest.sh" "$future_legacy_stable" "$public_key"
expect_failure "legacy candidate tags are accepted only for stable v0.1.0" \
    bash "$repo_root/release/verify-manifest.sh" \
    "$future_legacy_stable" stable v0.2.0 0.2.0 "$public_key"

unexpected_asset="$temporary_root/unexpected-asset"
cp -R "$stable_bundle" "$unexpected_asset"
printf 'not part of the release contract\n' > "$unexpected_asset/unexpected.txt"
expect_failure "unexpected release asset" \
    bash "$repo_root/release/verify-manifest.sh" \
    "$unexpected_asset" stable v0.1.0 0.1.0 "$public_key"

echo "release contract tests passed"
