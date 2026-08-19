#!/usr/bin/env bash
set -euo pipefail

version="${1:-0.1.1-dev}"
repo_root="$(cd "$(dirname "$0")/../../.." && pwd)"
package_root="$repo_root/node-gui/build/package-linux"
output_dir="$repo_root/node-gui/build/installer"

rm -rf "$package_root"
mkdir -p "$package_root/DEBIAN" "$package_root/usr/bin" \
  "$package_root/usr/lib/zonn" "$package_root/usr/share/applications" "$output_dir"

mkdir -p "$package_root/usr/share/icons/hicolor/512x512/apps"

install -m 0755 "$repo_root/node-gui/build/bin/zonn-node" "$package_root/usr/bin/zonn-node"
install -m 0755 "$repo_root/node-gui/build/worker/inference-worker" "$package_root/usr/lib/zonn/inference-worker"
install -m 0644 "$repo_root/llm-runner/inference_worker.py" "$package_root/usr/lib/zonn/inference_worker.py"
install -m 0644 "$repo_root/node-gui/build/linux/zonn-node.desktop" \
  "$package_root/usr/share/applications/zonn-node.desktop"
install -m 0644 "$repo_root/node-gui/build/appicon.png" \
  "$package_root/usr/share/icons/hicolor/512x512/apps/zonn-node.png"

cat > "$package_root/DEBIAN/control" <<EOF
Package: zonn-node
Version: $version
Section: utils
Priority: optional
Architecture: amd64
Maintainer: Zonn
Depends: libwebkit2gtk-4.1-0, libgtk-3-0
Description: Zonn contributor node
 Contribute CPU, memory, and GPU capacity to a Zonn Zone.
EOF

dpkg-deb --root-owner-group --build "$package_root" \
  "$output_dir/zonn-node_${version}_amd64.deb"
