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
            echo "warning: glibc ${GLIBC_VERSION} detected; nightly binaries need glibc 2.38+ and may not run." >&2
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

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "${TMP_DIR}"' EXIT

# This public key is the immutable trust root for signed release assets.
cat > "${TMP_DIR}/release-public-key.pem" <<'EOF'
-----BEGIN PUBLIC KEY-----
MIIBojANBgkqhkiG9w0BAQEFAAOCAY8AMIIBigKCAYEAsIRfz0Yf8P79QOHVw7pt
DIf7elx4c4/pbCnl7W4VBrj43uE6iFqExbWOCUImJ9dX1q0Hwj6jZV7mU5j8RGyw
zYy0lIacu5Qrf7Op5cueaPUCnA09PCVIgHya1U0TDmjvG6IgIDgYuwPiedXN5lvg
37K3YzYl4wFVES21iaN+6MvQg6U//5vpdCxkuY/zgg4jKOl3sSelcP49vc3d5+fO
nYhbF9ONYPpqI0/diKjdZkQiQxrnhhzgq4hbyyvekd16j1G9iEMyePVCxdTMeUqS
/O02do5ppZ1bfybPb3L90FRiq6Bt4eipu5TUxqOro/JFfbbfjHWUtc/9N0aT8lod
Ofm6iWdW0FDYHYqnSTPXJTxCb3aIntBM2xk3BgQt7Nmg0HzB6bkY9/Eih4w8u355
kVXeZtycjGDIQB0Q7hbwl1VGYUG5GrClffrz5ucphRiq+LoBYqjuF5RE8m7zQlVN
Llh6RGEhXrgdAx2smPphk4VXm7XVSmm7ETWkFHMVqdxxAgMBAAE=
-----END PUBLIC KEY-----
EOF

echo "Downloading MemPalace ${CHANNEL} (${PLATFORM})..."
fetch "${RELEASE_URL}/${CLI_ASSET}" "${TMP_DIR}/${CLI_ASSET}"
fetch "${RELEASE_URL}/${MCP_ASSET}" "${TMP_DIR}/${MCP_ASSET}"
fetch "${RELEASE_URL}/SHA256SUMS" "${TMP_DIR}/SHA256SUMS"
fetch "${RELEASE_URL}/SHA256SUMS.sig" "${TMP_DIR}/SHA256SUMS.sig"
fetch "${RELEASE_URL}/release-manifest.json" "${TMP_DIR}/release-manifest.json"
fetch "${RELEASE_URL}/release-manifest.sig" "${TMP_DIR}/release-manifest.sig"

# --- Checksum verification --------------------------------------------------
command -v openssl >/dev/null 2>&1 || err "openssl is required to verify the signed release manifest"
openssl dgst -sha256 -verify "${TMP_DIR}/release-public-key.pem" -signature "${TMP_DIR}/release-manifest.sig" "${TMP_DIR}/release-manifest.json" >/dev/null || err "release manifest signature verification FAILED — aborting install"
openssl dgst -sha256 -verify "${TMP_DIR}/release-public-key.pem" -signature "${TMP_DIR}/SHA256SUMS.sig" "${TMP_DIR}/SHA256SUMS" >/dev/null || err "release checksum signature verification FAILED — aborting install"
grep -Eq "\"channel\": \"${CHANNEL}\"" "${TMP_DIR}/release-manifest.json" || err "release manifest channel does not match requested channel"
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
