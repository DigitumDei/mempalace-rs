#!/usr/bin/env bash
# Exercise installer ordering offline. Cryptographic verification itself is
# covered with real signatures by release-contract.sh; downloads/processes are
# stubbed here so no test can migrate the operator's installation.
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
test_root="$(mktemp -d)"
trap 'rm -rf "$test_root"' EXIT
mkdir -p "$test_root/tools" "$test_root/assets" "$test_root/install space"
export AGENTPALACE_INSTALL_TEST_ASSETS="$test_root/assets"
export AGENTPALACE_INSTALL_TEST_LOG="$test_root/calls"
cat > "$test_root/tools/curl" <<'EOF'
#!/usr/bin/env bash
cp "$AGENTPALACE_INSTALL_TEST_ASSETS/${4##*/}" "$3"
EOF
cat > "$test_root/tools/uname" <<'EOF'
#!/usr/bin/env bash
if [ "$1" = '-s' ]; then echo Linux; else echo x86_64; fi
EOF
cat > "$test_root/tools/ldd" <<'EOF'
#!/usr/bin/env bash
echo 'ldd (GNU libc) 2.38'
EOF
cat > "$test_root/tools/openssl" <<'EOF'
#!/usr/bin/env bash
exit "${AGENTPALACE_INSTALL_TEST_VERIFY_FAIL:-0}"
EOF
cat > "$test_root/assets/agentpalace-linux-x86_64" <<'EOF'
#!/usr/bin/env bash
[ "$1" = migrate ] || exit 91
printf '%s\n' "$@" >> "$AGENTPALACE_INSTALL_TEST_LOG"
exit "${AGENTPALACE_INSTALL_TEST_MIGRATE_FAIL:-0}"
EOF
chmod +x "$test_root/tools/"* "$test_root/assets/agentpalace-linux-x86_64"
printf 'notices\n' > "$test_root/assets/agentpalace-notices-linux-x86_64.txt"
(cd "$test_root/assets" && sha256sum agentpalace-linux-x86_64 agentpalace-notices-linux-x86_64.txt > SHA256SUMS)
digest="$(sha256sum "$test_root/assets/SHA256SUMS" | cut -d' ' -f1)"
cat > "$test_root/assets/release-manifest.json" <<EOF
{
  "schema_version": 2,
  "channel": "stable",
  "version": "0.1.0",
  "commit_sha": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
  "release_tag": "v0.1.0",
  "source_candidate_tag": "v0.1.0-nightly.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
  "checksums_sha256": "$digest"
}
EOF
touch "$test_root/assets/SHA256SUMS.sig" "$test_root/assets/release-manifest.sig"
export PATH="$test_root/tools:$PATH"
args=(--no-setup --no-path --install-dir "$test_root/install space" --migrate-from "$test_root/old home" --migrate-to "$test_root/new home")
printf 'keep\n' > "$test_root/install space/existing"
if AGENTPALACE_INSTALL_TEST_VERIFY_FAIL=1 bash "$repo_root/install.sh" "${args[@]}" > "$test_root/output" 2>&1; then
    echo 'installer accepted failed verification' >&2; exit 1
fi
[ ! -e "$AGENTPALACE_INSTALL_TEST_LOG" ]
[ ! -e "$test_root/install space/agentpalace" ]
if AGENTPALACE_INSTALL_TEST_MIGRATE_FAIL=1 bash "$repo_root/install.sh" "${args[@]}" > "$test_root/output" 2>&1; then
    echo 'installer ignored migration failure' >&2; exit 1
fi
[ ! -e "$test_root/install space/agentpalace" ]
[ "$(cat "$test_root/install space/existing")" = keep ]
: > "$AGENTPALACE_INSTALL_TEST_LOG"
bash "$repo_root/install.sh" "${args[@]}" > "$test_root/output" 2>&1
[ -x "$test_root/install space/agentpalace" ]
[ "$(grep -c '^migrate$' "$AGENTPALACE_INSTALL_TEST_LOG")" = 1 ]
grep -Fx "$test_root/old home" "$AGENTPALACE_INSTALL_TEST_LOG" >/dev/null
grep -Fx "$test_root/new home" "$AGENTPALACE_INSTALL_TEST_LOG" >/dev/null
grep -Fx "$test_root/install space/agentpalace" "$AGENTPALACE_INSTALL_TEST_LOG" >/dev/null
bash "$repo_root/install.sh" "${args[@]}" > "$test_root/output" 2>&1
[ "$(grep -c '^migrate$' "$AGENTPALACE_INSTALL_TEST_LOG")" = 2 ]
[ "$(cat "$test_root/install space/existing")" = keep ]
echo 'installer rename tests passed'
