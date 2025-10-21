#!/usr/bin/env bash
set -euo pipefail

# ==============================================================================
#  Custom NAR Build Script for Snowflake OpenFlow / Hatch-Datavolo-NAR
# ==============================================================================

PROJECT_NAME="openflow-opcua-processors"
CUSTOM_BUILDER="./scripts/custom_builder.py"  # your custom builder
DIST_DIR="dist"

# ------------------------------------------------------------------------------
# Step 1: Clean old build artifacts
# ------------------------------------------------------------------------------
echo "🧹 Cleaning old build artifacts..."
rm -rf "$DIST_DIR" build *.egg-info || true

# ------------------------------------------------------------------------------
# Step 2: Ensure Hatch and plugin are up to date
# ------------------------------------------------------------------------------
echo "🐍 Ensuring Hatch + hatch-datavolo-nar are installed..."
pip install --upgrade hatch hatch-datavolo-nar hatchling

# ------------------------------------------------------------------------------
# Step 3: Locate Hatch global plugin env (the real one)
# ------------------------------------------------------------------------------
echo "🔍 Locating global Hatch builder environment..."
HATCH_ENV_PATH=$(find ~/.local/share/hatch/env -type d -path "*hatch-datavolo-nar*" -print -quit || true)

if [ -z "$HATCH_ENV_PATH" ]; then
  echo "⚠️ No existing Hatch env found for hatch-datavolo-nar; creating one..."
  hatch build || true  # triggers environment creation
  HATCH_ENV_PATH=$(find ~/.local/share/hatch/env -type d -path "*hatch-datavolo-nar*" -print -quit)
fi

if [ -z "$HATCH_ENV_PATH" ]; then
  echo "❌ Could not locate Hatch builder environment!"
  exit 1
fi

echo "✅ Found Hatch env: $HATCH_ENV_PATH"

# ------------------------------------------------------------------------------
# Step 4: Locate builder.py inside that environment
# ------------------------------------------------------------------------------
BUILDER_PATH=$(find "$HATCH_ENV_PATH" -type f -path "*/site-packages/hatch_datavolo_nar/builder.py" -print -quit)

if [ -z "$BUILDER_PATH" ]; then
  echo "❌ Could not locate builder.py inside Hatch environment!"
  exit 1
fi

echo "🧩 Found builder.py at: $BUILDER_PATH"

# ------------------------------------------------------------------------------
# Step 5: Backup and patch builder.py
# ------------------------------------------------------------------------------
BACKUP_PATH="${BUILDER_PATH}.orig"

if [ ! -f "$BACKUP_PATH" ]; then
  cp "$BUILDER_PATH" "$BACKUP_PATH"
fi

if [ -f "$CUSTOM_BUILDER" ]; then
  cp "$CUSTOM_BUILDER" "$BUILDER_PATH"
  echo "✅ Applied custom builder.py"
else
  echo "❌ Custom builder not found at $CUSTOM_BUILDER"
  exit 1
fi

# ------------------------------------------------------------------------------
# Step 6: Build using Hatch (uses the global builder we just patched)
# ------------------------------------------------------------------------------
echo "🏗️  Building NAR with custom builder..."
hatch build -t nar

# ------------------------------------------------------------------------------
# Step 7: Show output
# ------------------------------------------------------------------------------
echo "📦 Build artifacts:"
ls -lh "$DIST_DIR"
echo "✅ Done"
