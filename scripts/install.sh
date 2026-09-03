#!/usr/bin/env sh
# kqq installer — detects platform, downloads the right release asset.
# Usage: curl -fsSL https://raw.githubusercontent.com/kabnet-tech/kqq/main/scripts/install.sh | sh
set -eu

REPO="kabnet-tech/kqq"
VERSION="${KQQ_VERSION:-0.9.0}"

# Detect OS
os=$(uname -s)
case "$os" in
    Linux) os_name="linux" ;;
    Darwin) os_name="macos" ;;
    FreeBSD) os_name="freebsd" ;;
    *) echo "error: unsupported OS '$os'" >&2; exit 1 ;;
esac

# Detect architecture
arch=$(uname -m)
case "$arch" in
    x86_64|amd64) arch_name="x86_64" ;;
    aarch64|arm64) arch_name="aarch64" ;;
    armv6l|armv7l) arch_name="armv7hf" ;;
    i386|i686) arch_name="i386" ;;
    riscv64) arch_name="riscv64" ;;
    ppc64le) arch_name="ppc64le" ;;
    *) echo "error: unsupported architecture '$arch'" >&2; exit 1 ;;
esac

asset="kqq-${VERSION}-${os_name}-${arch_name}.tar.gz"
url="https://github.com/${REPO}/releases/download/v${VERSION}/${asset}"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

echo "==> Downloading ${asset}"
curl -fLo "${tmp}/${asset}" "$url"

echo "==> Verifying checksum"
curl -fLo "${tmp}/checksums.txt" "https://github.com/${REPO}/releases/download/v${VERSION}/checksums.txt"
(cd "$tmp" && grep " ${asset}\$" checksums.txt | sha256sum -c -)

echo "==> Installing"
tar -xzf "${tmp}/${asset}" -C "$tmp"
dest="${KQQ_INSTALL_DIR:-/usr/local/bin}"
if [ -n "${KQQ_INSTALL_DIR:-}" ]; then mkdir -p "$dest"; fi
if [ ! -w "$dest" ]; then
    dest="${HOME}/.local/bin"
    mkdir -p "$dest"
    echo "    (no write access to /usr/local/bin — installing to ${dest})"
fi
mv "${tmp}/kqq" "$dest/kqq"
chmod +x "$dest/kqq"

echo "==> Installed: $dest/kqq"
"$dest/kqq" --version
case ":$PATH:" in
    *":$dest:"*) ;;
    *) echo "note: add $dest to your PATH" ;;
esac
