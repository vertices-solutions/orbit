#!/usr/bin/env sh
set -eu

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

if ! command -v just >/dev/null 2>&1; then
	echo "error: missing required command: just" >&2
	exit 1
fi

exec just release "$@"
