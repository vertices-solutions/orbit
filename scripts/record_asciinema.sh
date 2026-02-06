#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

ASCIINEMA_BIN="${ASCIINEMA_BIN:-asciinema}"
ORBIT_BIN="${ORBIT_BIN:-orbit}"

CLUSTER_NAME="${CLUSTER_NAME:-winery}"
DESTINATION="${DESTINATION:-pino@winery.taild6e100.ts.net}"
DEFAULT_BASE_PATH="${DEFAULT_BASE_PATH:-~/runs}"

PROJECT_PATH="${PROJECT_PATH:-$ROOT_DIR/tests/93_hello_world}"
SBATCH_SCRIPT="${SBATCH_SCRIPT:-submit.sbatch}"

CAST_PATH="${1:-$ROOT_DIR/results/orbit-demo.cast}"

TITLE="${TITLE:-Orbit CLI demo: add cluster, submit job, inspect logs}"
WINDOW_SIZE="${WINDOW_SIZE:-110x30}"
IDLE_TIME_LIMIT="${IDLE_TIME_LIMIT:-2}"

IDENTITY_PATH="${IDENTITY_PATH:-}"
if [[ -z "$IDENTITY_PATH" ]]; then
	if [[ -f "$HOME/.ssh/id_ed25519" ]]; then
		IDENTITY_PATH="$HOME/.ssh/id_ed25519"
	elif [[ -f "$HOME/.ssh/id_rsa" ]]; then
		IDENTITY_PATH="$HOME/.ssh/id_rsa"
	else
		echo "IDENTITY_PATH is not set and no default SSH key was found." >&2
		exit 1
	fi
fi

PYTHON_BIN="${PYTHON_BIN:-}"
if [[ -z "$PYTHON_BIN" ]]; then
	if command -v python3 >/dev/null 2>&1; then
		PYTHON_BIN="python3"
	elif command -v python >/dev/null 2>&1; then
		PYTHON_BIN="python"
	else
		echo "python3 or python is required for JSON parsing." >&2
		exit 1
	fi
fi

if ! command -v "$ASCIINEMA_BIN" >/dev/null 2>&1; then
	echo "asciinema is not available in PATH." >&2
	exit 1
fi

if ! command -v "$ORBIT_BIN" >/dev/null 2>&1; then
	echo "orbit is not available in PATH." >&2
	exit 1
fi

if [[ ! -d "$PROJECT_PATH" ]]; then
	echo "Project path not found: $PROJECT_PATH" >&2
	exit 1
fi

if [[ ! -f "$PROJECT_PATH/$SBATCH_SCRIPT" ]]; then
	echo "Sbatch script not found: $PROJECT_PATH/$SBATCH_SCRIPT" >&2
	exit 1
fi

if ! "$ORBIT_BIN" ping >/dev/null 2>&1; then
	echo "orbitd is not reachable. Start the daemon before recording." >&2
	exit 1
fi

mkdir -p "$(dirname "$CAST_PATH")"

tmp_dir="$(mktemp -d)"
cleanup() {
	rm -rf "$tmp_dir"
}
trap cleanup EXIT

demo_script="$tmp_dir/orbit-demo-session.sh"
cat >"$demo_script" <<'DEMO'
#!/usr/bin/env bash
set -euo pipefail

ORBIT_BIN="${ORBIT_BIN:-orbit}"
CLUSTER_NAME="${CLUSTER_NAME:-winery}"
DESTINATION="${DESTINATION:-pino@winery.taild6e100.ts.net}"
IDENTITY_PATH="${IDENTITY_PATH:-~/.ssh/id_ed25519}"
DEFAULT_BASE_PATH="${DEFAULT_BASE_PATH:-~/runs}"
PROJECT_PATH="${PROJECT_PATH:-./tests/93_hello_world}"
SBATCH_SCRIPT="${SBATCH_SCRIPT:-submit.sbatch}"
PYTHON_BIN="${PYTHON_BIN:-python3}"

prompt() {
  printf "\n$ %s\n" "$*"
}

explain() {
  printf "\n# %s\n" "$*"
}

run() {
  prompt "$*"
  "$@"
}

pause() {
  sleep "${1:-0.8}"
}

clear
printf "Orbit demo: add cluster, submit job, check state, view logs\n"

pause
explain "Add the winery cluster (all parameters provided)."
run "$ORBIT_BIN" cluster add "$DESTINATION" \
  --name "$CLUSTER_NAME" \
  --identity-path "$IDENTITY_PATH" \
  --default-base-path "$DEFAULT_BASE_PATH"

pause
explain "Submit the example project."
run "$ORBIT_BIN" job submit --to "$CLUSTER_NAME" "$PROJECT_PATH" \
  --sbatchscript "$SBATCH_SCRIPT"

pause
explain "List jobs for the cluster."
run "$ORBIT_BIN" job list --cluster "$CLUSTER_NAME"

pause
explain "Resolve the latest job ID for this project."
jobs_json="$("$ORBIT_BIN" job list --cluster "$CLUSTER_NAME" --json)"
job_id="$("$PYTHON_BIN" -c $'import json, os, sys\n\ndata = json.load(sys.stdin)\nproject = os.environ.get("PROJECT_PATH") or ""\ntarget = os.path.abspath(project) if project else \"\"\n\ndef score(item):\n    try:\n        return int(item.get(\"job_id\") or 0)\n    except Exception:\n        return 0\n\ncandidates = []\nfor item in data:\n    local_path = item.get(\"local_path\") or \"\"\n    if target and os.path.abspath(local_path) == target:\n        candidates.append(item)\n\nif not candidates:\n    candidates = data\n\nif not candidates:\n    raise SystemExit(\"no jobs found for cluster\")\n\ncandidates.sort(key=score, reverse=True)\nprint(candidates[0][\"job_id\"])' <<<"$jobs_json")"

pause
explain "Check the job state."
run "$ORBIT_BIN" job get "$job_id"

pause
explain "Show job logs."
run "$ORBIT_BIN" job logs "$job_id"
DEMO

chmod +x "$demo_script"

export ORBIT_BIN
export CLUSTER_NAME
export DESTINATION
export IDENTITY_PATH
export DEFAULT_BASE_PATH
export PROJECT_PATH
export SBATCH_SCRIPT
export PYTHON_BIN

"$ASCIINEMA_BIN" rec \
	--overwrite \
	--title "$TITLE" \
	--window-size "$WINDOW_SIZE" \
	--idle-time-limit "$IDLE_TIME_LIMIT" \
	--capture-env "SHELL,TERM,USER" \
	--command "$demo_script" \
	--return \
	"$CAST_PATH"
