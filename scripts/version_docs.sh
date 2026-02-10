#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

require_cmd() {
	if ! command -v "$1" >/dev/null 2>&1; then
		echo "error: missing required command: $1" >&2
		exit 1
	fi
}

require_cmd python3
require_cmd npm

if [[ $# -ne 1 ]]; then
	echo "usage: scripts/version_docs.sh <version>" >&2
	exit 1
fi

version="$1"
if [[ ! "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
	echo "error: version must be in X.Y.Z format (got: $version)" >&2
	exit 1
fi

if [[ -f website/versions.json ]]; then
	if python3 - "$version" <<'PY'
import json
import sys
from pathlib import Path

version = sys.argv[1]
path = Path('website/versions.json')
versions = json.loads(path.read_text())
if version in versions:
    raise SystemExit(0)
raise SystemExit(1)
PY
	then
		echo "error: docs version already exists: $version" >&2
		exit 1
	fi
fi

if [[ ! -d website/node_modules ]]; then
	echo "error: website/node_modules is missing. Run 'cd website && npm install' first." >&2
	exit 1
fi

echo "Generating CLI docs..."
python3 scripts/generate_cli_docs.py

echo "Creating docs version $version..."
npm --prefix website run docs:version -- "$version"

echo "Docs version created: $version"
