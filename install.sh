#!/usr/bin/env bash
# MemPalace installer — downloads the stable build for this platform, verifies
# its signed manifest and checksums, installs to ~/.mempalace/bin, and registers the MCP
# server with detected AI tools.
#
#   curl -fsSL https://raw.githubusercontent.com/DigitumDei/mempalace-rs/main/install.sh | sh
#
# Options (pass via `sh -s -- <flags>` when piping):
#   --no-setup           skip `mempalace-cli setup` (MCP registration)
#   --no-path            skip adding the install dir to your shell PATH
#   --install-dir <dir>  install somewhere other than ~/.mempalace/bin
#   --channel <channel>  stable (default) or explicit nightly candidate
#   --version <tag>      required immutable nightly-<full-commit-sha> tag for nightly

set -eu

REPO="DigitumDei/mempalace-rs"
INSTALL_DIR="${HOME}/.mempalace/bin"
RUN_SETUP=1
UPDATE_PATH=1
CHANNEL="stable"
VERSION=""

while [ $# -gt 0 ]; do
    case "$1" in
        --no-setup) RUN_SETUP=0 ;;
        --no-path) UPDATE_PATH=0 ;;
        --install-dir)
            shift
            [ $# -gt 0 ] || { echo "error: --install-dir requires a value" >&2; exit 1; }
            INSTALL_DIR="$1"
            ;;
        --channel)
            shift
            [ $# -gt 0 ] || { echo "error: --channel requires a value" >&2; exit 1; }
            CHANNEL="$1"
            ;;
        --version)
            shift
            [ $# -gt 0 ] || { echo "error: --version requires a value" >&2; exit 1; }
            VERSION="$1"
            ;;
        -h|--help)
            cat <<'EOF'
MemPalace installer — downloads the stable build for this platform, verifies
its signed manifest and checksums, installs to ~/.mempalace/bin, and registers the MCP
server with detected AI tools.

Options (pass via `sh -s -- <flags>` when piping):
  --no-setup           skip `mempalace-cli setup` (MCP registration)
  --no-path            skip adding the install dir to your shell PATH
  --install-dir <dir>  install somewhere other than ~/.mempalace/bin
  --channel <channel>  stable (default) or explicit nightly candidate
  --version <tag>      required immutable nightly-<full-commit-sha> tag for nightly
EOF
            exit 0
            ;;
        *) echo "error: unknown option: $1" >&2; exit 1 ;;
    esac
    shift
done

err() { echo "error: $*" >&2; exit 1; }

case "$CHANNEL" in
    stable) [ -z "$VERSION" ] || err "--version is only supported with --channel nightly"; RELEASE_URL="https://github.com/${REPO}/releases/latest/download" ;;
    nightly)
        NIGHTLY_SHA="${VERSION#nightly-}"
        [ "${NIGHTLY_SHA}" != "${VERSION}" ] && [ "${#NIGHTLY_SHA}" -eq 40 ] || err "nightly updates require --version nightly-<full-commit-sha>"
        case "${NIGHTLY_SHA}" in
            *[!0123456789abcdef]*) err "nightly updates require --version nightly-<full-commit-sha>" ;;
        esac
        RELEASE_URL="https://github.com/${REPO}/releases/download/${VERSION}"
        ;;
    *) err "unsupported channel: $CHANNEL (expected stable or nightly)" ;;
esac

# --- Platform detection -----------------------------------------------------
OS="$(uname -s)"
ARCH="$(uname -m)"
case "${OS}/${ARCH}" in
    Linux/x86_64) PLATFORM="linux-x86_64" ;;
    Darwin/arm64) PLATFORM="macos-arm64" ;;
    Darwin/x86_64)
        err "Intel macOS is not supported (no prebuilt ONNX Runtime). Build from source instead: https://github.com/${REPO}/blob/main/docs/Quickstart.md"
        ;;
    *)
        err "unsupported platform: ${OS}/${ARCH}. Nightly builds cover Linux x86_64 (glibc 2.38+) and macOS Apple Silicon. Build from source instead: https://github.com/${REPO}/blob/main/docs/Quickstart.md"
        ;;
esac

if [ "${PLATFORM}" = "linux-x86_64" ] && command -v ldd >/dev/null 2>&1; then
    GLIBC_VERSION="$(ldd --version 2>/dev/null | head -n1 | grep -oE '[0-9]+\.[0-9]+' | head -n1 || true)"
    if [ -n "${GLIBC_VERSION}" ]; then
        GLIBC_MAJOR="${GLIBC_VERSION%%.*}"
        GLIBC_MINOR="${GLIBC_VERSION#*.}"
        if [ "${GLIBC_MAJOR}" -lt 2 ] || { [ "${GLIBC_MAJOR}" -eq 2 ] && [ "${GLIBC_MINOR}" -lt 38 ]; }; then
            echo "warning: glibc ${GLIBC_VERSION} detected; release binaries need glibc 2.38+ and may not run." >&2
        fi
    fi
fi

CLI_ASSET="mempalace-cli-${PLATFORM}"
MCP_ASSET="mempalace-mcp-${PLATFORM}"

# --- Downloader -------------------------------------------------------------
if command -v curl >/dev/null 2>&1; then
    fetch() { curl -fsSL -o "$2" "$1"; }
elif command -v wget >/dev/null 2>&1; then
    fetch() { wget -q -O "$2" "$1"; }
else
    err "neither curl nor wget found; install one and retry"
fi

if command -v sha256sum >/dev/null 2>&1; then
    file_sha256() { sha256sum "$1" | cut -d' ' -f1; }
elif command -v shasum >/dev/null 2>&1; then
    file_sha256() { shasum -a 256 "$1" | cut -d' ' -f1; }
else
    err "sha256sum or shasum is required to verify release assets"
fi

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "${TMP_DIR}"' EXIT

# This public key is the immutable trust root for signed release assets.
cat > "${TMP_DIR}/release-public-key.pem" <<'EOF'
-----BEGIN PUBLIC KEY-----
MIICIjANBgkqhkiG9w0BAQEFAAOCAg8AMIICCgKCAgEAqDpP5+PmejB/5RA2bO/K
qNyoDx06467nK8h0zakY/3ev7YmewAirWA3y4ZmOhR+1qTJMTL82coYTUmr7VTZq
lGsYaOMWGitcmvBU/8veQPKEORZvSAttOo+Qpaz8e6XYWKya6m2Rtrds9CUYP1vi
q3v+nJ6kvKHLLjeWdp9Fs1G/jvNDgWYKuvAXJUW0dDHROs6LIviqcevP9flsNiXQ
GmfZP7vfskSHxszw9JRSxcf5pXhoPuaSmUKikzTR9vPW1/1u4fXpUmWNC6mARszG
jUf3HkrBFL1RQ3EMGyU6vEUBAk9qis7aohDTXjBOiNdsL4BChAuKUOHYNIIcgyW0
88s6RiiRrMl23nL0BK+2llUp3tdzVxf7pwfdGEJv9e7GJbBcv2nG0S9jXpl+xPoJ
hJ0t3mdPzcj0scl6pAYW/8dgww1M1LjDHXrEi1P/JCaAZC4s+k9o3czw89YlQZ5U
WMXwIoMYztwYF/Yj4Wh26LRuAF5GLRBFwPfRh3WWFocuxV6PLCKdxVDxomsn7o0l
I6qHiGeOfmejxWGHCA5d8ZOZ/mXMtto43EzP0CFC4++DgHncoMPbTVXPhE06Zasn
2kE7sDXPmMJYzq6XU2l7t/mAvzmBF/EBixYrnUZwSRje/PfvVPB5ZvMjRsJPxCoA
dJDS7qZOD9CIIsZX+6HOd48CAwEAAQ==
-----END PUBLIC KEY-----
EOF

echo "Downloading MemPalace ${CHANNEL} (${PLATFORM})..."
download_release_asset() {
    asset="$1"
    if ! fetch "${RELEASE_URL}/${asset}" "${TMP_DIR}/${asset}"; then
        if [ "$CHANNEL" = "stable" ]; then
            err "no complete stable release is available; retry after the first signed stable release is published"
        fi
        err "candidate ${VERSION} is unavailable or incomplete"
    fi
}
download_release_asset "${CLI_ASSET}"
download_release_asset "${MCP_ASSET}"
download_release_asset "SHA256SUMS"
download_release_asset "SHA256SUMS.sig"
download_release_asset "release-manifest.json"
download_release_asset "release-manifest.sig"

# --- Checksum verification --------------------------------------------------
command -v openssl >/dev/null 2>&1 || err "openssl is required to verify the signed release manifest"
openssl dgst -sha256 -verify "${TMP_DIR}/release-public-key.pem" -signature "${TMP_DIR}/release-manifest.sig" "${TMP_DIR}/release-manifest.json" >/dev/null || err "release manifest signature verification FAILED — aborting install"
openssl dgst -sha256 -verify "${TMP_DIR}/release-public-key.pem" -signature "${TMP_DIR}/SHA256SUMS.sig" "${TMP_DIR}/SHA256SUMS" >/dev/null || err "release checksum signature verification FAILED — aborting install"

manifest_string_field() {
    field="$1"
    values="$(sed -n "s/^[[:space:]]*\"${field}\"[[:space:]]*:[[:space:]]*\"\\([^\"]*\\)\"[[:space:]]*,\\{0,1\\}[[:space:]]*$/\\1/p" "${TMP_DIR}/release-manifest.json")"
    [ -n "$values" ] || err "release manifest is missing ${field}"
    [ "$(printf '%s\n' "$values" | wc -l | tr -d '[:space:]')" -eq 1 ] \
        || err "release manifest contains duplicate ${field} fields"
    printf '%s' "$values"
}

grep -Eq '^[[:space:]]*"schema_version"[[:space:]]*:[[:space:]]*2,' "${TMP_DIR}/release-manifest.json" \
    || err "unsupported release manifest schema"
MANIFEST_CHANNEL="$(manifest_string_field channel)"
MANIFEST_VERSION="$(manifest_string_field version)"
MANIFEST_COMMIT="$(manifest_string_field commit_sha)"
MANIFEST_TAG="$(manifest_string_field release_tag)"
MANIFEST_CHECKSUMS_SHA256="$(manifest_string_field checksums_sha256)"

[ "$MANIFEST_CHANNEL" = "$CHANNEL" ] || err "release manifest channel does not match requested channel"
printf '%s\n' "$MANIFEST_VERSION" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?(\+[0-9A-Za-z.-]+)?$' \
    || err "release manifest contains an invalid version"
printf '%s\n' "$MANIFEST_COMMIT" | grep -Eq '^[0-9a-f]{40}$' \
    || err "release manifest contains an invalid commit SHA"
printf '%s\n' "$MANIFEST_CHECKSUMS_SHA256" | grep -Eq '^[0-9a-f]{64}$' \
    || err "release manifest contains an invalid checksum-manifest digest"

case "$CHANNEL" in
    nightly)
        [ "$MANIFEST_TAG" = "$VERSION" ] \
            || err "release manifest tag does not match requested candidate"
        [ "$MANIFEST_COMMIT" = "$NIGHTLY_SHA" ] \
            || err "release manifest commit does not match requested candidate"
        ;;
    stable)
        [ "$MANIFEST_TAG" = "v${MANIFEST_VERSION}" ] \
            || err "stable release manifest tag does not match its version"
        MANIFEST_SOURCE_CANDIDATE="$(manifest_string_field source_candidate_tag)"
        [ "$MANIFEST_SOURCE_CANDIDATE" = "nightly-${MANIFEST_COMMIT}" ] \
            || err "stable release manifest is not bound to its source candidate"
        ;;
esac

ACTUAL_CHECKSUMS_SHA256="$(file_sha256 "${TMP_DIR}/SHA256SUMS")"
[ "$ACTUAL_CHECKSUMS_SHA256" = "$MANIFEST_CHECKSUMS_SHA256" ] \
    || err "signed release manifest does not match SHA256SUMS"

echo "Verifying signed manifest and checksums..."
grep -E "^[0-9a-fA-F]{64} [ *](${CLI_ASSET}|${MCP_ASSET})\$" "${TMP_DIR}/SHA256SUMS" \
    > "${TMP_DIR}/SHA256SUMS.filtered" || true
[ "$(wc -l < "${TMP_DIR}/SHA256SUMS.filtered")" -eq 2 ] \
    || err "SHA256SUMS is missing entries for ${PLATFORM} assets"
if command -v sha256sum >/dev/null 2>&1; then
    (cd "${TMP_DIR}" && sha256sum -c SHA256SUMS.filtered >/dev/null) \
        || err "checksum verification FAILED — aborting install"
else
    (cd "${TMP_DIR}" && shasum -a 256 -c SHA256SUMS.filtered >/dev/null) \
        || err "checksum verification FAILED — aborting install"
fi

# --- Install ----------------------------------------------------------------
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

# --- PATH -------------------------------------------------------------------
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

# --- MCP setup ----------------------------------------------------------------
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
