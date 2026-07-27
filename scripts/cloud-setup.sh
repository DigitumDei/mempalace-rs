#!/usr/bin/env bash
# Bootstrap a cloud sandbox (Claude Code on the web, or any fresh Ubuntu box)
# for building and testing the mempalace-rs workspace.
#
# Idempotent: safe to re-run, and safe to use as a snapshot/setup script.
# See docs/Cloud-Environment.md for the firewall allowlist and env vars this
# script assumes are in place.

set -euo pipefail

log() { printf '==> %s\n' "$*"; }

SUDO=""
if [ "$(id -u)" -ne 0 ]; then
	SUDO="sudo"
fi

# 1. System packages.
#    protobuf-compiler: required by mempalace-storage via lancedb/lance.
#    build-essential:   rusqlite uses the `bundled` feature, so SQLite is
#                       compiled from source.
#    libssl-dev:        openssl-sys is not vendored (no openssl-src in
#                       Cargo.lock), so it links the system OpenSSL via
#                       pkg-config and needs the headers, which a minimal
#                       image carrying only the runtime does not have.
log "Installing system packages"
$SUDO apt-get update
$SUDO apt-get install -y --no-install-recommends \
	build-essential \
	ca-certificates \
	curl \
	git \
	libssl-dev \
	pkg-config \
	protobuf-compiler

# 2. Rust toolchain. The workspace is edition 2024 with rust-version 1.88, and
#    there is no rust-toolchain.toml, so pin stable explicitly.
if ! command -v rustup >/dev/null 2>&1; then
	log "Installing rustup"
	curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
		| sh -s -- -y --default-toolchain stable --profile minimal
fi

# rustup's installer writes this; sourcing it is a no-op when cargo is already
# on PATH from a previous run.
# shellcheck disable=SC1091
[ -f "$HOME/.cargo/env" ] && . "$HOME/.cargo/env"

log "Ensuring stable toolchain with clippy and rustfmt"
rustup toolchain install stable --profile minimal --component clippy --component rustfmt
rustup default stable

MIN_RUST="1.88.0"
ACTUAL_RUST="$(rustc --version | cut -d' ' -f2)"
if [ "$(printf '%s\n%s\n' "$MIN_RUST" "$ACTUAL_RUST" | sort -V | head -n1)" != "$MIN_RUST" ]; then
	echo "error: rustc $ACTUAL_RUST is older than the required $MIN_RUST" >&2
	exit 1
fi
log "rustc $ACTUAL_RUST"

# 3. Warm the build. `cargo fetch` pulls every crate in Cargo.lock; the
#    `cargo check` that follows runs the ort-sys build script, which downloads
#    a prebuilt ONNX Runtime from cdn.pyke.io into ~/.cache/ort.pyke.io.
#    Snapshot after this so later sessions need no network for either.
log "Fetching dependencies"
cargo fetch --locked

log "Warming the build (this is the long pole on a cold box)"
cargo check --workspace --all-targets --locked

log "Cloud environment ready"
