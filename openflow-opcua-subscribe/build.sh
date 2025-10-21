#!/usr/bin/env bash
echo "=== Clear Build ==="
deactivate
rm -rf dist/ .venv/ .hatch/

echo "=== Setting up virtual environment ==="
python3.12 -m venv .venv
source .venv/bin/activate
echo ""

echo "=== Upgrading dependencies ==="
pip install --upgrade pip
pip install hatch hatch-datavolo-nar
echo ""

echo "=== Building NAR ==="
hatch build -t nar
echo ""

echo "=== Build complete. ==="

