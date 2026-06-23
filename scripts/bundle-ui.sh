#!/usr/bin/env bash
# Build the monitoring UI (no local Node — done inside Docker) and bundle it as
# ui.zip in the CLI's resources so `scheduler` ships and serves the UI. Run this
# before packaging the CLI whenever the UI changes. Assumes the scheduler repo
# is a sibling of scheduler-sdk.
set -euo pipefail
cd "$(dirname "$0")/.."

UI_DIR="../scheduler/scheduler-ui"
DEST="scheduler-cli/src/main/resources/ui.zip"

"$UI_DIR/build-dist.sh"
rm -f "$DEST"
( cd "$UI_DIR/dist" && zip -qr - . ) > "$DEST"

echo
echo "Bundled UI → $DEST"
