#!/usr/bin/env python3
import argparse
import hashlib
import json
import os
import re
import shlex
import shutil
import socket
import subprocess
import sys
import time
import uuid
from pathlib import Path
import signal


def run_cmd(cmd, cwd=None, capture=True):
    result = subprocess.run(
        cmd,
        cwd=cwd,
        text=True,
        capture_output=capture,
    )
    if result.returncode != 0:
        msg = (result.stdout or "") + (result.stderr or "")
        raise RuntimeError(f"command failed ({result.returncode}): {' '.join(cmd)}\n{msg}")
    return result


def run_cmd_status(cmd):
    result = subprocess.run(
        cmd,
        text=True,
        capture_output=True,
    )
    return result.returncode, (result.stdout or "") + (result.stderr or "")


def command_with_config(command, config_path):
    cmd = shlex.split(command)
    if config_path is None:
        return cmd
    if not any(arg == "--config" or arg.startswith("--config=") for arg in cmd):
        cmd.extend(["--config", str(config_path)])
    return cmd


DEFAULT_ORBITD_PORT = 50056


def load_toml_config(path: Path) -> dict:
    try:
        import tomllib
    except ModuleNotFoundError:  # pragma: no cover - fallback for older Python
        tomllib = None
    if tomllib is not None:
        with path.open("rb") as handle:
            data = tomllib.load(handle)
        return data if isinstance(data, dict) else {}

    try:
        import tomli as tomllib  # type: ignore[import-not-found]
    except ModuleNotFoundError:
        tomllib = None

    if tomllib is not None:
        with path.open("rb") as handle:
            data = tomllib.load(handle)
        return data if isinstance(data, dict) else {}

    data: dict = {}
    for raw_line in path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.split("#", 1)[0].strip()
        if not key or not value:
            continue
        if value.startswith(("\"", "'")) and value.endswith(("\"", "'")):
            data[key] = value[1:-1]
            continue
        lowered = value.lower()
        if lowered in ("true", "false"):
            data[key] = lowered == "true"
            continue
        try:
            data[key] = int(value)
        except ValueError:
            data[key] = value
    return data


def port_available(port: int) -> bool:
    if port <= 0 or port > 65535:
        return False
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        try:
            sock.bind(("127.0.0.1", port))
        except OSError:
            return False
    return True


def allocate_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def maybe_isolate_config(config_path: Path, out_dir: Path) -> Path:
    config_data = load_toml_config(config_path)
    port = config_data.get("port", DEFAULT_ORBITD_PORT)
    if not isinstance(port, int):
        raise RuntimeError("config port must be an integer")
    if port_available(port):
        return config_path

    isolated_port = allocate_free_port()
    db_dir = out_dir / "db"
    db_dir.mkdir(parents=True, exist_ok=True)
    db_path = db_dir / f"orbit_{uuid.uuid4().hex[:8]}.sqlite"
    config_lines = [
        f"port = {isolated_port}",
        f"database_path = \"{db_path}\"",
    ]
    job_interval = config_data.get("job_check_interval_secs")
    if isinstance(job_interval, int):
        config_lines.append(f"job_check_interval_secs = {job_interval}")
    verbose = config_data.get("verbose")
    if isinstance(verbose, bool):
        config_lines.append(f"verbose = {'true' if verbose else 'false'}")

    isolated_path = out_dir / f"config.e2e.{isolated_port}.toml"
    isolated_path.write_text("\n".join(config_lines) + "\n")
    print(
        f"config port {port} is in use; using isolated config {isolated_path} "
        f"(port {isolated_port})"
    )
    source_db = config_data.get("database_path")
    if isinstance(source_db, str):
        source_db_path = Path(source_db)
        if not source_db_path.is_absolute():
            source_db_path = config_path.parent / source_db_path
        if source_db_path.exists():
            shutil.copy2(source_db_path, db_path)
            for suffix in ("-wal", "-shm"):
                extra = Path(str(source_db_path) + suffix)
                if extra.exists():
                    shutil.copy2(extra, Path(str(db_path) + suffix))
    return isolated_path


def start_daemon(daemon_cmd, ping_cmd, timeout_secs, poll_secs):
    proc = subprocess.Popen(daemon_cmd, start_new_session=True)
    deadline = time.monotonic() + timeout_secs
    last_error = None
    while True:
        if proc.poll() is not None:
            raise RuntimeError("orbitd exited before becoming ready")
        status, output = run_cmd_status(ping_cmd)
        if status == 0:
            return proc
        last_error = output.strip() or f"status {status}"
        if time.monotonic() >= deadline:
            raise RuntimeError(f"orbitd did not become ready: {last_error}")
        time.sleep(poll_secs)


def stop_daemon(proc, timeout_secs=10):
    if proc is None:
        return
    if proc.poll() is not None:
        return
    try:
        if os.name == "nt":
            proc.terminate()
        else:
            os.killpg(proc.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    except PermissionError:
        try:
            proc.terminate()
        except Exception:
            return
    try:
        proc.wait(timeout=timeout_secs)
    except subprocess.TimeoutExpired:
        if os.name == "nt":
            proc.kill()
        else:
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except PermissionError:
                proc.kill()
        proc.wait(timeout=timeout_secs)


ANSI_RE = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")


def strip_ansi(text):
    return ANSI_RE.sub("", text)


def parse_job_id(output):
    cleaned = strip_ansi(output)
    patterns = [
        r"Job\s+(\d+)\s+submitted",
        r"job get\s+(\d+)",
    ]
    for pattern in patterns:
        match = re.search(pattern, cleaned)
        if match:
            return int(match.group(1))
    raise RuntimeError("unable to parse job id from run output")


def parse_remote_path(output):
    cleaned = strip_ansi(output)
    for line in cleaned.splitlines():
        if "Remote path:" in line:
            _, _, path = line.partition("Remote path:")
            path = path.strip()
            if path:
                return path
    raise RuntimeError("unable to parse remote path from run output")


def parse_orbit_json(output):
    data = json.loads(output)
    if isinstance(data, dict) and "ok" in data:
        if not data.get("ok"):
            if "reason" in data:
                message = data.get("reason") or "orbit returned an error"
            else:
                error = data.get("error", {})
                message = error.get("message") or "orbit returned an error"
            raise RuntimeError(message)
        return data.get("result")
    return data


def parse_json_output(result):
    stdout = (result.stdout or "").strip()
    if stdout:
        try:
            return parse_orbit_json(stdout)
        except json.JSONDecodeError:
            pass
    stderr = (result.stderr or "").strip()
    if stderr:
        return parse_orbit_json(stderr)
    raise RuntimeError("missing JSON output")


def parse_orbit_error_output(output):
    cleaned = output.strip()
    if not cleaned:
        raise RuntimeError("missing JSON error output")
    start = cleaned.find("{")
    end = cleaned.rfind("}")
    if start == -1 or end == -1 or end <= start:
        raise RuntimeError(f"unable to parse JSON error output:\n{cleaned}")
    payload = cleaned[start : end + 1]
    data = json.loads(payload)
    if not isinstance(data, dict) or data.get("ok") is not False:
        raise RuntimeError(f"unexpected error payload:\n{cleaned}")
    return data


def parse_run_result(result):
    stdout = (result.stdout or "").strip()
    if stdout:
        try:
            data = parse_orbit_json(stdout)
            if isinstance(data, dict):
                job_id = data.get("job_id")
                if job_id is not None:
                    return int(job_id), data.get("remote_path")
        except json.JSONDecodeError:
            pass
    stderr = (result.stderr or "").strip()
    if stderr:
        try:
            data = parse_orbit_json(stderr)
            if isinstance(data, dict):
                job_id = data.get("job_id")
                if job_id is not None:
                    return int(job_id), data.get("remote_path")
        except json.JSONDecodeError:
            pass

    output = (result.stdout or "") + (result.stderr or "")
    return parse_job_id(output), parse_remote_path(output)


def parse_run_json(result):
    data = parse_json_output(result)
    if not isinstance(data, dict):
        raise RuntimeError("run JSON should be an object")
    job_id = data.get("job_id")
    if job_id is None:
        raise RuntimeError("run JSON missing job_id")
    return int(job_id), data.get("remote_path")


def combined_stream_text(data):
    return (data.get("stdout") or "") + (data.get("stderr") or "")


def job_status(orbit_cmd, job_id):
    cmd = list(orbit_cmd)
    if "--non-interactive" not in cmd:
        cmd.append("--non-interactive")
    result = run_cmd(cmd + ["job", "get", str(job_id)])
    data = parse_json_output(result)
    return data.get("status"), data.get("terminal_state")


def list_jobs_json(orbit_cmd, cluster=None, blueprint=None):
    cmd = list(orbit_cmd)
    if "--non-interactive" not in cmd:
        cmd.append("--non-interactive")
    cmd.extend(["job", "list"])
    if cluster:
        cmd.extend(["--cluster", cluster])
    if blueprint:
        cmd.extend(["--blueprint", blueprint])
    data = parse_json_output(run_cmd(cmd))
    if not isinstance(data, list):
        raise RuntimeError("job list JSON should be an array")
    return data


def wait_for_job_list_entry(orbit_cmd, cluster, job_id, timeout_secs, poll_secs):
    deadline = time.monotonic() + timeout_secs
    while True:
        jobs = list_jobs_json(orbit_cmd, cluster=cluster)
        if any(item.get("job_id") == job_id for item in jobs):
            return jobs
        if time.monotonic() >= deadline:
            raise RuntimeError(f"job {job_id} not found in job list")
        time.sleep(poll_secs)


def validate_cluster_list_flags(orbit_cmd, cluster_name):
    cmd = list(orbit_cmd)
    if "--non-interactive" not in cmd:
        cmd.append("--non-interactive")

    default_result = parse_json_output(run_cmd(cmd + ["cluster", "list"]))
    if not isinstance(default_result, list):
        raise RuntimeError("cluster list: expected array output")
    default_cluster = next(
        (item for item in default_result if item.get("name") == cluster_name),
        None,
    )
    if default_cluster is None:
        raise RuntimeError(f"cluster list: cluster '{cluster_name}' is not configured")
    if "reachable" in default_cluster:
        raise RuntimeError("cluster list: reachable must be omitted without --check-reachability")

    with_reachability = parse_json_output(
        run_cmd(cmd + ["cluster", "list", "--check-reachability"])
    )
    if not isinstance(with_reachability, list):
        raise RuntimeError("cluster list --check-reachability: expected array output")
    checked_cluster = next(
        (item for item in with_reachability if item.get("name") == cluster_name),
        None,
    )
    if checked_cluster is None:
        raise RuntimeError(
            f"cluster list --check-reachability: cluster '{cluster_name}' is not configured"
        )
    if "reachable" not in checked_cluster:
        raise RuntimeError(
            "cluster list --check-reachability: reachable is missing from response"
        )


def validate_blueprint_job_filter(orbit_cmd, cluster, blueprint_name, expected_job_id):
    jobs = list_jobs_json(orbit_cmd, cluster=cluster, blueprint=blueprint_name)
    if not jobs:
        raise RuntimeError(
            f"job list --blueprint {blueprint_name}: expected at least one job in response"
        )
    if not any(item.get("job_id") == expected_job_id for item in jobs):
        raise RuntimeError(
            f"job list --blueprint {blueprint_name}: missing submitted job {expected_job_id}"
        )
    mismatched = [
        item.get("job_id")
        for item in jobs
        if item.get("blueprint_name") != blueprint_name
    ]
    if mismatched:
        raise RuntimeError(
            f"job list --blueprint {blueprint_name}: received mismatched jobs {mismatched}"
        )


def validate_directory_job_has_no_blueprint(orbit_cmd, cluster, job_id):
    jobs = list_jobs_json(orbit_cmd, cluster=cluster)
    record = next((item for item in jobs if item.get("job_id") == job_id), None)
    if record is None:
        raise RuntimeError(f"job list: job {job_id} missing while validating blueprint metadata")
    if record.get("blueprint_name") is not None:
        raise RuntimeError(
            f"directory run job {job_id} unexpectedly has blueprint_name="
            f"{record.get('blueprint_name')!r}"
        )


def wait_for_job(orbit_cmd, job_id, timeout_secs, poll_secs):
    deadline = time.monotonic() + timeout_secs
    while True:
        status, terminal_state = job_status(orbit_cmd, job_id)
        if status == "completed":
            return
        if status == "failed":
            raise RuntimeError(f"job {job_id} failed (terminal_state={terminal_state})")
        if time.monotonic() >= deadline:
            raise RuntimeError(f"timed out waiting for job {job_id}")
        time.sleep(poll_secs)


def wait_for_job_canceled(orbit_cmd, job_id, timeout_secs, poll_secs):
    deadline = time.monotonic() + timeout_secs
    while True:
        status, terminal_state = job_status(orbit_cmd, job_id)
        if status == "canceled":
            if terminal_state not in ("CANCELED", "CANCELLED"):
                raise RuntimeError(
                    f"job {job_id} canceled with unexpected terminal_state={terminal_state}"
                )
            return
        if status == "completed":
            raise RuntimeError(f"job {job_id} completed before cancel")
        if status == "failed":
            raise RuntimeError(f"job {job_id} failed before cancel (terminal_state={terminal_state})")
        if time.monotonic() >= deadline:
            raise RuntimeError(f"timed out waiting for job {job_id} cancel")
        time.sleep(poll_secs)


def cleanup_job_and_validate(orbit_cmd, cluster, job_id, remote_path):
    if not remote_path:
        raise RuntimeError(f"missing remote path for job {job_id}")
    cleanup_cmd = orbit_cmd + ["job", "cleanup", str(job_id), "--yes"]
    run_cmd(cleanup_cmd)
    ls_cmd = orbit_cmd + ["cluster", "ls", cluster, remote_path]
    status, output = run_cmd_status(ls_cmd)
    if status == 0:
        raise RuntimeError(
            f"cleanup did not remove remote path for job {job_id}: {remote_path}\n{output}"
        )


def sha256_path(path):
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8192), b""):
            digest.update(chunk)
    return digest.hexdigest()


def validate_smoke(project_out, repo_root):
    results_dir = project_out / "results"
    line_count = (results_dir / "line_count.txt").read_text().strip().split()
    if not line_count:
        raise RuntimeError("smoke: line_count.txt empty")
    if int(line_count[0]) != 4:
        raise RuntimeError("smoke: line_count.txt expected 4 lines")

    expected_hash = sha256_path(repo_root / "tests/01_smoke/data/input.txt")
    actual_hash = (results_dir / "input.sha256").read_text().split()[0]
    if actual_hash != expected_hash:
        raise RuntimeError("smoke: input.sha256 mismatch")


def validate_smoke_logs(orbit_cmd, job_id):
    stdout_logs = run_cmd(orbit_cmd + ["job", "logs", str(job_id)])
    stdout_output = (stdout_logs.stdout or "") + (stdout_logs.stderr or "")
    if "smoke run on" not in stdout_output:
        raise RuntimeError("smoke: stdout logs missing expected output")

    stderr_logs = run_cmd(orbit_cmd + ["job", "logs", str(job_id), "--err"])
    stderr_output = (stderr_logs.stdout or "") + (stderr_logs.stderr or "")
    if "stderr check: this should show up in --err logs" not in stderr_output:
        raise RuntimeError("smoke: stderr logs missing expected output")


def validate_smoke_logs_json(orbit_cmd, job_id):
    stdout_logs = parse_json_output(run_cmd(orbit_cmd + ["job", "logs", str(job_id)]))
    stdout_output = combined_stream_text(stdout_logs)
    if "smoke run on" not in stdout_output:
        raise RuntimeError("smoke: stdout logs missing expected output (json)")

    stderr_logs = parse_json_output(
        run_cmd(orbit_cmd + ["job", "logs", str(job_id), "--err"])
    )
    stderr_output = combined_stream_text(stderr_logs)
    if "stderr check: this should show up in --err logs" not in stderr_output:
        raise RuntimeError("smoke: stderr logs missing expected output (json)")


def validate_python_stats(project_out):
    results_dir = project_out / "results"
    stats = json.loads((results_dir / "stats.json").read_text())
    if stats.get("count") != 6:
        raise RuntimeError("python_stats: count mismatch")
    if stats.get("min") != 1.0 or stats.get("max") != 7.75:
        raise RuntimeError("python_stats: min/max mismatch")
    mean = stats.get("mean")
    if mean is None or abs(mean - 4.0) > 1e-6:
        raise RuntimeError("python_stats: mean mismatch")

    preview_lines = (results_dir / "preview.txt").read_text().splitlines()
    if not preview_lines or preview_lines[0] != "values:":
        raise RuntimeError("python_stats: preview header missing")
    if len(preview_lines) < 2 or preview_lines[1] != "- 3.5":
        raise RuntimeError("python_stats: preview first value mismatch")


def validate_filter_tree(project_out):
    results_file = project_out / "files.txt"
    if not results_file.is_file():
        raise RuntimeError("filter_tree: files.txt not retrieved to output root")
    legacy_path = project_out / "results" / "files.txt"
    if legacy_path.exists():
        raise RuntimeError("filter_tree: retrieve prefix was not stripped")
    raw_lines = results_file.read_text().splitlines()
    files = [line.strip().lstrip("./") for line in raw_lines if line.strip()]

    required = {
        "data/keep.txt",
        "data/nested/keep.csv",
        "cache/keep.txt",
        "notes/readme.md",
        "run.sh",
        "submit.sbatch",
    }
    excluded = {
        "data/ignore.tmp",
        "cache/scratch.tmp",
        "build/artifact.o",
    }

    missing = required.difference(files)
    if missing:
        raise RuntimeError(f"filter_tree: missing files: {sorted(missing)}")
    leaked = excluded.intersection(files)
    if leaked:
        raise RuntimeError(f"filter_tree: excluded files present: {sorted(leaked)}")


def validate_binary_output(project_out, repo_root):
    bin_path = project_out / "random.bin"
    sha_path = project_out / "random.sha256"
    if not bin_path.is_file():
        raise RuntimeError("binary_output: random.bin not retrieved to output root")
    if not sha_path.is_file():
        raise RuntimeError("binary_output: random.sha256 not retrieved to output root")
    legacy_bin = project_out / "results" / "raw" / "random.bin"
    if legacy_bin.exists():
        raise RuntimeError("binary_output: retrieve prefix was not stripped for random.bin")
    actual_hash = sha_path.read_text().split()[0]
    expected_hash = sha256_path(bin_path)
    if actual_hash != expected_hash:
        raise RuntimeError("binary_output: random.bin hash mismatch")

    sample_src = (repo_root / "tests/04_binary_output/data/sample.txt").read_text()
    sample_copy_path = project_out / "text" / "sample_copy.txt"
    if not sample_copy_path.is_file():
        raise RuntimeError("binary_output: text/sample_copy.txt not retrieved to output root")
    legacy_sample = project_out / "results" / "text" / "sample_copy.txt"
    if legacy_sample.exists():
        raise RuntimeError("binary_output: retrieve prefix was not stripped for sample_copy.txt")
    sample_copy = sample_copy_path.read_text()
    if sample_src != sample_copy:
        raise RuntimeError("binary_output: sample_copy.txt mismatch")


def validate_templated_hashes(project_out, expected_count, expected_method):
    results_dir = project_out / "results"
    summary_path = results_dir / "summary.txt"
    if not summary_path.is_file():
        raise RuntimeError("templated_hashes: summary.txt missing")
    summary_lines = summary_path.read_text().splitlines()
    method_line = next(
        (line for line in summary_lines if line.startswith("hash_method: ")),
        None,
    )
    if method_line != f"hash_method: {expected_method}":
        raise RuntimeError("templated_hashes: hash_method mismatch")
    files_line = next(
        (line for line in summary_lines if line.startswith("files: ")),
        None,
    )
    if files_line != f"files: {expected_count}":
        raise RuntimeError("templated_hashes: file count mismatch")

    hashes_path = results_dir / f"hashes.{expected_method}"
    if not hashes_path.is_file():
        raise RuntimeError("templated_hashes: hashes file missing")
    hash_lines = [line for line in hashes_path.read_text().splitlines() if line.strip()]
    if len(hash_lines) != expected_count:
        raise RuntimeError("templated_hashes: hashes count mismatch")

    inputs_path = results_dir / "inputs.txt"
    if not inputs_path.is_file():
        raise RuntimeError("templated_hashes: inputs.txt missing")
    input_lines = inputs_path.read_text().splitlines()
    if len(input_lines) < 2:
        raise RuntimeError("templated_hashes: inputs.txt incomplete")
    if input_lines[0] != f"Number of files: {expected_count}":
        raise RuntimeError("templated_hashes: inputs.txt file count mismatch")
    if input_lines[1] != f"Hashing algorithm: {expected_method}":
        raise RuntimeError("templated_hashes: inputs.txt hash method mismatch")


def build_run_cmd(orbit_cmd, cluster, project_path, run_args, extra_args=None):
    cmd = orbit_cmd + [
        "run",
        str(project_path),
        "--on",
        cluster,
    ]
    cmd.extend(run_args)
    if extra_args:
        cmd.extend(extra_args)
    return cmd


def main():
    parser = argparse.ArgumentParser(description="Run orbit end-to-end test scenarios.")
    parser.add_argument("--cluster", required=True, help="Cluster name configured in orbit.")
    parser.add_argument(
        "--orbit-bin",
        default="cargo run -p orbit --",
        help="orbit CLI command (supports quoted strings).",
    )
    parser.add_argument(
        "--orbitd-bin",
        default="cargo run -p orbitd --",
        help="orbitd command (supports quoted strings).",
    )
    parser.add_argument(
        "--out-dir",
        default="tests/_out",
        help="Local output directory for retrieved results.",
    )
    parser.add_argument(
        "--keep",
        action="store_true",
        help="Keep retrieved outputs under --out-dir.",
    )
    parser.add_argument(
        "--config",
        default="tests/config.toml",
        help="Path to the orbit/orbitd config file.",
    )
    parser.add_argument("--timeout", type=int, default=600, help="Timeout per job in seconds.")
    parser.add_argument(
        "--daemon-timeout",
        type=int,
        default=240,
        help="Timeout for orbitd startup in seconds.",
    )
    parser.add_argument("--poll", type=int, default=3, help="Polling interval in seconds.")
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    config_path = Path(args.config)
    if not config_path.is_absolute():
        config_path = (repo_root / config_path).resolve()
    if not config_path.is_file():
        raise RuntimeError(f"config file not found: {config_path}")
    out_dir = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    config_path = maybe_isolate_config(config_path, out_dir)

    orbit_cmd = command_with_config(args.orbit_bin, config_path)
    orbitd_cmd = command_with_config(args.orbitd_bin, config_path)
    non_interactive_cmd = orbit_cmd + ["--non-interactive"]
    keep_outputs = args.keep

    daemon_proc = None
    try:
        daemon_proc = start_daemon(
            orbitd_cmd,
            orbit_cmd + ["ping"],
            args.daemon_timeout,
            args.poll,
        )
        run_cmd(orbit_cmd + ["ping"])
        validate_cluster_list_flags(non_interactive_cmd, args.cluster)
        print("cluster list flags: ok")

        scenarios = [
            {
                "id": "01_smoke",
                "path": repo_root / "tests/01_smoke",
                "run_args": [],
                "retrieve": ["results"],
                "validate": lambda out: validate_smoke(out, repo_root),
            },
            {
                "id": "02_python_stats",
                "path": repo_root / "tests/02_python_stats",
                "run_args": ["--sbatchscript", "scripts/submit.sbatch"],
                "retrieve": ["results"],
                "validate": validate_python_stats,
            },
            {
                "id": "03_filter_tree",
                "path": repo_root / "tests/03_filter_tree",
                "run_args": [
                    "--include",
                    "cache/keep.txt",
                    "--exclude",
                    "cache/*",
                    "--exclude",
                    "*.tmp",
                    "--exclude",
                    "build/",
                ],
                "retrieve": ["results/files.txt"],
                "validate": validate_filter_tree,
            },
            {
                "id": "04_binary_output",
                "path": repo_root / "tests/04_binary_output",
                "run_args": [],
                "retrieve": [
                    "results/raw/random.bin",
                    "results/raw/random.sha256",
                    "results/text",
                ],
                "validate": lambda out: validate_binary_output(out, repo_root),
            },
            {
                "id": "05_cancel_job",
                "path": repo_root / "tests/05_cancel_job",
                "run_args": [],
                "cancel": True,
                "retrieve": [],
                "validate": lambda out: None,
            },
            {
                "id": "06_templated_hashes",
                "path": repo_root / "tests/06_templated_hashes",
                "run_args": [
                    "--field",
                    "hash_method=sha256",
                    "--field",
                    "file_count=3",
                    "--fill-defaults",
                ],
                "retrieve": ["results"],
                "validate": lambda out: validate_templated_hashes(out, 3, "sha256"),
            },
        ]

        for scenario in scenarios:
            run_cmdline = build_run_cmd(
                orbit_cmd,
                args.cluster,
                scenario["path"],
                scenario["run_args"],
            )
            result = run_cmd(run_cmdline)
            job_id, remote_path = parse_run_result(result)
            job_ids = [job_id]
            job_paths = {job_id: remote_path}
            primary_job_id = job_id
            if scenario.get("cancel"):
                print(f"{scenario['id']}: submitted job {job_id}")
                cancel_cmd = orbit_cmd + ["job", "cancel", str(job_id), "--yes"]
                run_cmd(cancel_cmd)
                wait_for_job_canceled(orbit_cmd, job_id, args.timeout, args.poll)
                print(f"{scenario['id']}: job {job_id} canceled")
                cleanup_job_and_validate(
                    orbit_cmd, args.cluster, job_id, job_paths[job_id]
                )
                print(f"{scenario['id']}: cleanup ok for job {job_id}")
                continue
            if scenario["id"] == "01_smoke":
                conflict_cmd = build_run_cmd(
                    non_interactive_cmd,
                    args.cluster,
                    scenario["path"],
                    scenario["run_args"],
                )
                conflict_status, conflict_output = run_cmd_status(conflict_cmd)
                if conflict_status == 0:
                    raise RuntimeError(
                        "run should fail while a job is running in the same directory"
                    )
                conflict_error = parse_orbit_error_output(conflict_output)
                error_type = str(conflict_error.get("errorType") or "").lower()
                if error_type != "conflict":
                    raise RuntimeError(
                        "run conflict expected errorType=conflict "
                        f"(actual={conflict_error.get('errorType')})"
                    )
                reason = str(conflict_error.get("reason") or "")
                if not reason:
                    raise RuntimeError(
                        "run conflict message missing reason "
                        f"(reason={reason})"
                    )

                force_cmd = build_run_cmd(
                    orbit_cmd,
                    args.cluster,
                    scenario["path"],
                    scenario["run_args"],
                    extra_args=["--force"],
                )
                force_result = run_cmd(force_cmd)
                force_job_id, force_remote_path = parse_run_result(force_result)
                if force_remote_path != remote_path:
                    raise RuntimeError(
                        "force run did not reuse existing remote path"
                    )
                job_ids.append(force_job_id)
                job_paths[force_job_id] = force_remote_path

                new_dir_cmd = build_run_cmd(
                    orbit_cmd,
                    args.cluster,
                    scenario["path"],
                    scenario["run_args"],
                    extra_args=["--new-directory"],
                )
                new_dir_result = run_cmd(new_dir_cmd)
                new_dir_job_id, new_dir_remote_path = parse_run_result(new_dir_result)
                if new_dir_remote_path == remote_path:
                    raise RuntimeError(
                        "new-directory run did not create a new remote path"
                    )
                job_ids.append(new_dir_job_id)
                job_paths[new_dir_job_id] = new_dir_remote_path
                primary_job_id = new_dir_job_id

            for active_job_id in job_ids:
                print(f"{scenario['id']}: submitted job {active_job_id}")
                wait_for_job(orbit_cmd, active_job_id, args.timeout, args.poll)
                wait_for_job_list_entry(
                    non_interactive_cmd,
                    args.cluster,
                    active_job_id,
                    args.timeout,
                    args.poll,
                )
                validate_directory_job_has_no_blueprint(
                    non_interactive_cmd,
                    args.cluster,
                    active_job_id,
                )
                print(f"{scenario['id']}: job {active_job_id} completed")

            if scenario["id"] == "01_smoke":
                validate_smoke_logs(orbit_cmd, primary_job_id)
                print(f"{scenario['id']}: logs ok")

            project_out = out_dir / scenario["id"] / str(primary_job_id)
            project_out.mkdir(parents=True, exist_ok=True)

            for path in scenario["retrieve"]:
                retrieve_cmd = orbit_cmd + [
                    "job",
                    "retrieve",
                    str(primary_job_id),
                    path,
                    "--output",
                    str(project_out),
                    "--overwrite",
                ]
                run_cmd(retrieve_cmd)

            scenario["validate"](project_out)
            print(f"{scenario['id']}: validation ok")

            for cleanup_job_id in job_ids:
                cleanup_job_and_validate(
                    orbit_cmd,
                    args.cluster,
                    cleanup_job_id,
                    job_paths[cleanup_job_id],
                )
                print(f"{scenario['id']}: cleanup ok for job {cleanup_job_id}")

        blueprint_lifecycle_template = repo_root / "tests/93_hello_world"
        blueprint_lifecycle_src = out_dir / "_blueprint_lifecycle_src"
        if blueprint_lifecycle_src.exists():
            shutil.rmtree(blueprint_lifecycle_src)
        shutil.copytree(blueprint_lifecycle_template, blueprint_lifecycle_src)

        blueprint_name = f"e2e_blueprint_{uuid.uuid4().hex[:10]}"
        run_cmd(
            orbit_cmd
            + [
                "blueprint",
                "init",
                str(blueprint_lifecycle_src),
                "--name",
                blueprint_name,
            ]
        )

        listed = parse_json_output(run_cmd(non_interactive_cmd + ["blueprint", "list"]))
        blueprints_payload = listed.get("blueprints") or []
        if any(item.get("name") == blueprint_name for item in blueprints_payload):
            raise RuntimeError("blueprint lifecycle: init should not register blueprint")

        build_result = parse_json_output(
            run_cmd(non_interactive_cmd + ["blueprint", "build", str(blueprint_lifecycle_src)])
        )
        blueprint_version = build_result.get("versionTag")
        if build_result.get("name") != blueprint_name or not blueprint_version:
            raise RuntimeError("blueprint lifecycle: build returned unexpected metadata")
        blueprint_ref = f"{blueprint_name}:{blueprint_version}"

        listed = parse_json_output(run_cmd(non_interactive_cmd + ["blueprint", "list"]))
        blueprints_payload = listed.get("blueprints") or []
        blueprint_record = next(
            (
                item
                for item in blueprints_payload
                if item.get("name") == blueprint_name
                and blueprint_version in (item.get("tags") or [])
            ),
            None,
        )
        if blueprint_record is None:
            raise RuntimeError("blueprint lifecycle: built blueprint missing from blueprint list")
        if blueprint_record.get("latest_tag") != blueprint_version:
            raise RuntimeError(
                "blueprint lifecycle: latest_tag did not match built version "
                f"(expected={blueprint_version}, actual={blueprint_record.get('latest_tag')})"
            )
        expected_blueprint_path = str(blueprint_lifecycle_src.resolve())
        if blueprint_record.get("path") != expected_blueprint_path:
            raise RuntimeError(
                "blueprint lifecycle: listed blueprint path mismatch "
                f"(expected={expected_blueprint_path}, actual={blueprint_record.get('path')})"
            )

        blueprint_run_cmd = non_interactive_cmd + [
            "blueprint",
            "run",
            blueprint_ref,
            "--on",
            args.cluster,
        ]
        blueprint_run = run_cmd(blueprint_run_cmd)
        blueprint_job_id, blueprint_remote_path = parse_run_json(blueprint_run)
        print(f"blueprint lifecycle: submitted job {blueprint_job_id}")

        status, terminal_state = job_status(orbit_cmd, blueprint_job_id)
        if status not in ("queued", "running", "completed"):
            raise RuntimeError(
                "blueprint lifecycle: unexpected initial job status "
                f"{status} (terminal_state={terminal_state})"
            )

        wait_for_job(orbit_cmd, blueprint_job_id, args.timeout, args.poll)
        status, terminal_state = job_status(orbit_cmd, blueprint_job_id)
        if status != "completed":
            raise RuntimeError(
                f"blueprint lifecycle: expected completed status, got {status} "
                f"(terminal_state={terminal_state})"
            )
        validate_blueprint_job_filter(
            non_interactive_cmd,
            args.cluster,
            blueprint_name,
            blueprint_job_id,
        )
        print("blueprint lifecycle: job list --blueprint filter ok")
        print(f"blueprint lifecycle: job {blueprint_job_id} completed")

        cleanup_job_and_validate(orbit_cmd, args.cluster, blueprint_job_id, blueprint_remote_path)
        print(f"blueprint lifecycle: cleanup ok for job {blueprint_job_id}")

        run_cmd(orbit_cmd + ["blueprint", "delete", blueprint_ref, "--yes"])
        listed_after_delete = parse_json_output(
            run_cmd(non_interactive_cmd + ["blueprint", "list"])
        )
        blueprints_after_delete = listed_after_delete.get("blueprints") or []
        if any(item.get("name") == blueprint_name for item in blueprints_after_delete):
            raise RuntimeError("blueprint lifecycle: blueprint still present after delete")
        print(f"blueprint lifecycle: deleted blueprint {blueprint_name}")

        ping_result = parse_json_output(run_cmd(non_interactive_cmd + ["ping"]))
        if ping_result.get("message") != "pong":
            raise RuntimeError("non-interactive ping returned unexpected response")

        non_interactive_run_dir = repo_root / "tests/01_smoke"
        run_cmdline = build_run_cmd(
            non_interactive_cmd,
            args.cluster,
            non_interactive_run_dir,
            [],
        )
        run_result = run_cmd(run_cmdline)
        job_id, remote_path = parse_run_json(run_result)
        if not remote_path:
            raise RuntimeError("non-interactive run missing remote_path")
        wait_for_job(non_interactive_cmd, job_id, args.timeout, args.poll)
        print(f"non-interactive: submitted job {job_id}")

        validate_smoke_logs_json(non_interactive_cmd, job_id)
        print("non-interactive: logs ok")

        non_interactive_out = out_dir / "non_interactive" / str(job_id)
        non_interactive_out.mkdir(parents=True, exist_ok=True)
        retrieve_cmd = non_interactive_cmd + [
            "job",
            "retrieve",
            str(job_id),
            "results",
            "--output",
            str(non_interactive_out),
            "--overwrite",
        ]
        run_cmd(retrieve_cmd)
        validate_smoke(non_interactive_out, repo_root)
        print("non-interactive: validation ok")

        print("All scenarios completed.")
        return 0
    finally:
        stop_daemon(daemon_proc)
        if not keep_outputs:
            shutil.rmtree(out_dir, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
