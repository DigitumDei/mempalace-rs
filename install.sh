#!/usr/bin/env bash
# MemPalace installer — downloads the latest **stable** release for this
# platform, verifies the signed manifest and checksums using the committed
# public key, installs to ~/.mempalace/bin, and registers the MCP server
# with detected AI tools.
#
# Usage (stable, default):
#   curl -fsSL https://raw.githubusercontent.com/DigitumDei/mempalace-rs/main/install.sh | sh
#
# Usage (immutable candidate / nightly, explicit tag):
#   curl -fsSL https://raw.githubusercontent.com/DigitumDei/mempalace-rs/main/install.sh | sh -s -- --channel nightly-<40-hex-commit-sha>
#
# Options (pass via `sh -s -- <flags>` when piping):
#   --channel <tag>      install channel: "stable" (default) or an explicit
#                        immutable candidate tag like "nightly-<40-hex-sha>".
#                        The tag must match a published GitHub Release.
#   --no-setup           skip `mempalace-cli setup` (MCP registration)
#   --no-path            skip adding the install dir to your shell PATH
#   --install-dir <dir>  install somewhere other than ~/.mempalace/bin

set -euo pipefail

REPO="DigitumDei/mempalace-rs"
INSTALL_DIR="${HOME}/.mempalace/bin"
RUN_SETUP=1
UPDATE_PATH=1
CHANNEL="stable"

while [ $# -gt 0 ]; do
    case "$1" in
        --channel)
            shift
            [ $# -gt 0 ] || { echo "error: --channel requires a value" >&2; exit 1; }
            CHANNEL="$1"
            ;;
        --no-setup) RUN_SETUP=0 ;;
        --no-path) UPDATE_PATH=0 ;;
        --install-dir)
            shift
            [ $# -gt 0 ] || { echo "error: --install-dir requires a value" >&2; exit 1; }
            INSTALL_DIR="$1"
            ;;
        -h|--help)
            cat <<'EOF'
MemPalace installer — downloads the latest stable release (or an explicit
candidate tag), verifies the signed manifest and checksums against the
committed public key, installs to ~/.mempalace/bin, and registers the MCP
server with detected AI tools.

Options (pass via `sh -s -- <flags>` when piping):
  --channel <tag>      install channel: "stable" (default) or an explicit
                       immutable candidate like "nightly-<40-hex-sha>".
  --no-setup           skip `mempalace-cli setup` (MCP registration)
  --no-path            skip adding the install dir to your shell PATH
  --install-dir <dir>  install somewhere other than ~/.mempalace/bin
EOF
            exit 0
            ;;
        *) echo "error: unknown option: $1" >&2; exit 1 ;;
    esac
    shift
done

err() { echo "error: $*" >&2; exit 1; }

# ---------------------------------------------------------------------------
# Public key — pinned in the installer source. This is the Ed25519 public key
# whose corresponding private key signs all release manifests and checksum
# files. Never fetched from the release server.
# ---------------------------------------------------------------------------
PUBLIC_KEY_PEM="-----BEGIN PUBLIC KEY-----
MCowBQYDK2VwAyEAXFaJde6SWshP25EyDG28lInqtXNRrW0fU4fbDyM/AQA=
-----END PUBLIC KEY-----"

# ---------------------------------------------------------------------------
# Platform detection
# ---------------------------------------------------------------------------
OS="$(uname -s)"
ARCH="$(uname -m)"
case "${OS}/${ARCH}" in
    Linux/x86_64) PLATFORM="linux-x86_64" ;;
    Darwin/arm64) PLATFORM="macos-arm64" ;;
    Darwin/x86_64)
        err "Intel macOS is not supported (no prebuilt ONNX Runtime). Build from source instead: https://github.com/${REPO}/blob/main/docs/Quickstart.md"
        ;;
    *)
        err "unsupported platform: ${OS}/${ARCH}. Supported: Linux x86_64 (glibc 2.38+), macOS Apple Silicon, Windows x86_64. Build from source: https://github.com/${REPO}/blob/main/docs/Quickstart.md"
        ;;
esac

if [ "${PLATFORM}" = "linux-x86_64" ] && command -v ldd >/dev/null 2>&1; then
    GLIBC_VERSION="$(ldd --version 2>/dev/null | head -n1 | grep -oE '[0-9]+\.[0-9]+' | head -n1 || true)"
    if [ -n "${GLIBC_VERSION}" ]; then
        GLIBC_MAJOR="${GLIBC_VERSION%%.*}"
        GLIBC_MINOR="${GLIBC_VERSION#*.}"
        if [ "${GLIBC_MAJOR}" -lt 2 ] || { [ "${GLIBC_MAJOR}" -eq 2 ] && [ "${GLIBC_MINOR}" -lt 38 ]; }; then
            echo "warning: glibc ${GLIBC_VERSION} detected; binaries need glibc 2.38+ and may not run." >&2
        fi
    fi
fi

CLI_ASSET="mempalace-cli-${PLATFORM}"
MCP_ASSET="mempalace-mcp-${PLATFORM}"

# ---------------------------------------------------------------------------
# Resolve channel — stable release tag or explicit candidate tag
# ---------------------------------------------------------------------------
if [ "${CHANNEL}" = "stable" ]; then
    # Fetch the latest stable release tag from GitHub API
    if command -v curl >/dev/null 2>&1; then
        RELEASE_TAG=$(curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" \
            | grep '"tag_name":' | head -1 | sed 's/.*: "\(.*\)",/\1/')
    elif command -v wget >/dev/null 2>&1; then
        RELEASE_TAG=$(wget -q -O - "https://api.github.com/repos/${REPO}/releases/latest" \
            | grep '"tag_name":' | head -1 | sed 's/.*: "\(.*\)",/\1/')
    else
        err "neither curl nor wget found; install one and retry"
    fi
    [ -n "${RELEASE_TAG}" ] || err "could not determine latest stable release tag"
    echo "Resolved latest stable release: ${RELEASE_TAG}"
else
    RELEASE_TAG="${CHANNEL}"
    # Validate channel is an immutable tag (either stable v* or nightly-<40hex>)
    if ! echo "${RELEASE_TAG}" | grep -qE '^v[0-9]+\.[0-9]+\.[0-9]+$' && \
       ! echo "${RELEASE_TAG}" | grep -qE '^nightly-[0-9a-f]{40}$'; then
        err "invalid channel '${CHANNEL}': must be 'stable' or an explicit tag like 'nightly-<40-hex-sha>'"
    fi
fi

RELEASE_URL="https://github.com/${REPO}/releases/download/${RELEASE_TAG}"

echo "Channel: ${CHANNEL} -> tag ${RELEASE_TAG}"

# ---------------------------------------------------------------------------
# Downloader
# ---------------------------------------------------------------------------
if command -v curl >/dev/null 2>&1; then
    fetch() { curl -fsSL -o "$2" "$1"; }
elif command -v wget >/dev/null 2>&1; then
    fetch() { wget -q -O "$2" "$1"; }
else
    err "neither curl nor wget found; install one and retry"
fi

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "${TMP_DIR}"' EXIT

echo "Downloading MemPalace ${RELEASE_TAG} (${PLATFORM})..."
fetch "${RELEASE_URL}/manifest.json" "${TMP_DIR}/manifest.json"
fetch "${RELEASE_URL}/manifest.json.sig" "${TMP_DIR}/manifest.json.sig"
fetch "${RELEASE_URL}/SHA256SUMS" "${TMP_DIR}/SHA256SUMS"
fetch "${RELEASE_URL}/SHA256SUMS.sig" "${TMP_DIR}/SHA256SUMS.sig"
fetch "${RELEASE_URL}/${CLI_ASSET}" "${TMP_DIR}/${CLI_ASSET}"
fetch "${RELEASE_URL}/${MCP_ASSET}" "${TMP_DIR}/${MCP_ASSET}"

# ---------------------------------------------------------------------------
# Signature verification — verify signatures over raw downloaded bytes.
# Both manifest.json and SHA256SUMS are signed with the release private key;
# we verify using the pinned public key before trusting any content.
# ---------------------------------------------------------------------------
echo "Verifying signatures..."

# Write the pinned public key to a temp file
PUBKEY_FILE="${TMP_DIR}/public-key.pem"
printf '%s\n' "${PUBLIC_KEY_PEM}" > "${PUBKEY_FILE}"

verify_sig() {
    local file="$1" sig="$2" label="$3"
    if [ ! -f "${file}" ]; then
        err "missing file for signature verification: ${file}"
    fi
    if [ ! -f "${sig}" ]; then
        err "missing signature file: ${sig}"
    fi
    if command -v openssl >/dev/null 2>&1; then
        openssl dgst -sha256 -verify "${PUBKEY_FILE}" \
            -signature "${sig}" "${file}" >/dev/null 2>&1 \
            || err "${label} signature verification FAILED — aborting install"
    elif command -v gpg >/dev/null 2>&1; then
        # Fallback: gpg can verify Ed25519 if the key is imported
        IMPORTED=$(gpg --import "${PUBKEY_FILE}" 2>&1 || true)
        gpg --verify "${sig}" "${file}" >/dev/null 2>&1 \
            || err "${label} signature verification FAILED — aborting install"
    else
        err "no openssl or gpg found — cannot verify signatures. Install openssl and retry."
    fi
    echo "  ✓ ${label} signature verified"
}

verify_sig "${TMP_DIR}/manifest.json" "${TMP_DIR}/manifest.json.sig" "manifest.json"
verify_sig "${TMP_DIR}/SHA256SUMS" "${TMP_DIR}/SHA256SUMS.sig" "SHA256SUMS"

# ---------------------------------------------------------------------------
# Validate manifest channel matches installation channel
# ---------------------------------------------------------------------------
echo "Validating manifest metadata..."
MANIFEST_CHANNEL=$(grep -o '"channel"[[:space:]]*:[[:space:]]*"[^"]*"' "${TMP_DIR}/manifest.json" \
    | head -1 | sed 's/.*: *"\(.*\)"/\1/')
MANIFEST_TAG=$(grep -o '"tag"[[:space:]]*:[[:space:]]*"[^"]*"' "${TMP_DIR}/manifest.json" \
    | head -1 | sed 's/.*: *"\(.*\)"/\1/')

if [ "${CHANNEL}" != "stable" ] && [ "${CHANNEL}" != "${MANIFEST_TAG}" ]; then
    err "manifest tag '${MANIFEST_TAG}' does not match requested channel tag '${CHANNEL}'"
fi
if [ "${CHANNEL}" = "stable" ] && [ "${MANIFEST_CHANNEL}" != "stable" ]; then
    err "manifest channel is '${MANIFEST_CHANNEL}', expected 'stable' for default install"
fi
echo "  ✓ Channel/version metadata validated (tag=${MANIFEST_TAG}, channel=${MANIFEST_CHANNEL})"

# ---------------------------------------------------------------------------
# Checksum verification — verify against the now-trusted SHA256SUMS
# ---------------------------------------------------------------------------
echo "Verifying checksums..."
# Filter SHA256SUMS for our platform assets
grep -E "^[0-9a-fA-F]{64} [ *](${CLI_ASSET}|${MCP_ASSET})\$" "${TMP_DIR}/SHA256SUMS" \
    > "${TMP_DIR}/SHA256SUMS.filtered" || true
[ "$(wc -l < "${TMP_DIR}/SHA256SUMS.filtered")" -eq 2 ] \
    || err "SHA256SUMS is missing entries for ${PLATFORM} assets"
if command -v sha256sum >/dev/null 2>&1; then
    (cd "${TMP_DIR}" && sha256sum -c SHA256SUMS.filtered >/dev/null) \
        || err "checksum verification FAILED — aborting install"
elif command -v shasum >/dev/null 2>&1; then
    (cd "${TMP_DIR}" && shasum -a 256 -c SHA256SUMS.filtered >/dev/null) \
        || err "checksum verification FAILED — aborting install"
else
    err "no sha256sum or shasum found — cannot verify checksums"
fi
echo "  ✓ Asset checksums verified"

# ---------------------------------------------------------------------------
# Install
# ---------------------------------------------------------------------------
UPDATED=0
[ -f "${INSTALL_DIR}/mempalace-cli" ] && UPDATED=1
mkdir -p "${INSTALL_DIR}"
mv "${TMP_DIR}/${CLI_ASSET}" "${INSTALL_DIR}/mempalace-cli"
mv "${TMP_DIR}/${MCP_ASSET}" "${INSTALL_DIR}/mempalace-mcp"
chmod +x "${INSTALL_DIR}/mempalace-cli" "${INSTALL_DIR}/mempalace-mcp"
ln -sf "${INSTALL_DIR}/mempalace-cli" "${INSTALL_DIR}/mempalace"

if [ "${UPDATED}" -eq 1 ]; then
    echo "Updated existing install in ${INSTALL_DIR}"
else
    echo "Installed mempalace-cli and mempalace-mcp to ${INSTALL_DIR}"
fi

# ---------------------------------------------------------------------------
# PATH
# ---------------------------------------------------------------------------
if [ "${UPDATE_PATH}" -eq 1 ]; then
    SHELL_NAME="$(basename "${SHELL:-sh}")"
    case "${SHELL_NAME}" in
        fish)
            if command -v fish >/dev/null 2>&1; then
                fish -c "fish_add_path '${INSTALL_DIR}'" >/dev/null 2>&1 \
                    && echo "Added ${INSTALL_DIR} to fish PATH" \
                    || echo "warning: could not update fish PATH; add ${INSTALL_DIR} manually" >&2
            fi
            ;;
        zsh) RC_FILE="${HOME}/.zshrc" ;;
        *) RC_FILE="${HOME}/.bashrc" ;;
    esac
    if [ "${SHELL_NAME}" != "fish" ]; then
        if [ -f "${RC_FILE}" ] && grep -q '\.mempalace/bin' "${RC_FILE}"; then
            : # already on PATH via rc file
        else
            printf '\n# Added by the MemPalace installer\nexport PATH="%s:$PATH"\n' "${INSTALL_DIR}" >> "${RC_FILE}"
            echo "Added ${INSTALL_DIR} to PATH in ${RC_FILE} — restart your shell to pick it up."
        fi
    fi
fi

# ---------------------------------------------------------------------------
# MCP setup
# ---------------------------------------------------------------------------
if [ "${RUN_SETUP}" -eq 1 ]; then
    "${INSTALL_DIR}/mempalace-cli" setup
else
    echo "Skipped MCP registration. Run it later with:"
    echo "  ${INSTALL_DIR}/mempalace-cli setup"
fi

cat <<EOF

MemPalace is installed. Next steps:
  mempalace-cli init /path/to/your/project    # create a palace for a project
  mempalace-cli mine /path/to/your/project    # ingest its files

Full walkthrough: https://github.com/${REPO}/blob/main/docs/Quickstart.md
EOF
