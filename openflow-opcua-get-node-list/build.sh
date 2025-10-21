#!/usr/bin/env bash
set -euo pipefail
python3 -V
pipx install hatch || pip install hatch
pip install hatch-datavolo-nar
hatch build -t nar
echo "Built NAR(s) in dist/"
