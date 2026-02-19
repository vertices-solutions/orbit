#!/usr/bin/env python3
import argparse
import hashlib
import json
import os
import re
import shlex
import shutil
import signal
import socket
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Sequence


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
    except ModuleNotFoundError:
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


def validate_run_conflict_error(payload, expected_remote_path=None):
    error_type = str(payload.get("errorType") or "").lower()
    if error_type not in ("conflict", "remote_error"):
        raise RuntimeError(
            "run conflict expected errorType in {conflict,remote_error} "
            f"(actual={payload.get('errorType')})"
        )

    reason = str(payload.get("reason") or "")
    details = payload.get("details")
    details_text = ""
    if isinstance(details, str):
        details_text = details
    elif details is not None:
        details_text = json.dumps(details)

    combined = f"{reason}\n{details_text}"
    lowered = combined.lower()
    markers = (
        "is still running in",
        "cancel it first or run in a new directory",
        "--new-directory",
        "--remote-path",
        "remote path",
        "in use by another job",
    )
    if not any(marker in lowered for marker in markers):
        raise RuntimeError(
            "run conflict response missing expected conflict context "
            f"(reason={reason!r}, details={details!r})"
        )

    if expected_remote_path and expected_remote_path in combined:
        return


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


def validate_cluster_set_key_value(orbit_cmd, cluster_name):
    cmd = list(orbit_cmd)
    if "--non-interactive" not in cmd:
        cmd.append("--non-interactive")

    run_cmd(cmd + ["cluster", "set", "--on", cluster_name, "default=true"])
    cluster = parse_json_output(run_cmd(cmd + ["cluster", "get", cluster_name]))
    if not isinstance(cluster, dict):
        raise RuntimeError("cluster get: expected object output")
    if cluster.get("name") != cluster_name:
        raise RuntimeError(
            f"cluster get: expected cluster '{cluster_name}', got {cluster.get('name')!r}"
        )
    if cluster.get("is_default") is not True:
        raise RuntimeError("cluster set: expected default=true to be applied")


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


@dataclass
class E2EContext:
    repo_root: Path
    out_dir: Path
    cluster: str
    timeout_secs: int
    poll_secs: int
    orbit_cmd: List[str]
    non_interactive_cmd: List[str]

    def run(self, cmd, cwd=None):
        return run_cmd(cmd, cwd=cwd)

    def run_status(self, cmd):
        return run_cmd_status(cmd)

    def build_project_run_cmd(
        self,
        project_path: Path,
        run_args: Sequence[str],
        *,
        extra_args: Optional[Sequence[str]] = None,
        non_interactive: bool = False,
    ):
        base_cmd = self.non_interactive_cmd if non_interactive else self.orbit_cmd
        return build_run_cmd(
            base_cmd,
            self.cluster,
            project_path,
            list(run_args),
            extra_args=list(extra_args) if extra_args else None,
        )


class E2ETestCase:
    def __init__(self, name: str, test_dir: Path):
        self.name = name
        self.test_dir = test_dir

    def setup(self, ctx: E2EContext):
        pass

    def run(self, ctx: E2EContext):
        raise NotImplementedError

    def cleanup(self, ctx: E2EContext):
        pass

    def execute(self, ctx: E2EContext):
        print(f"{self.name}: setup ({self.test_dir})")
        self.setup(ctx)

        run_error = None
        try:
            print(f"{self.name}: run")
            self.run(ctx)
            print(f"{self.name}: run ok")
        except Exception as exc:  # noqa: BLE001
            run_error = exc

        cleanup_error = None
        try:
            print(f"{self.name}: cleanup")
            self.cleanup(ctx)
            print(f"{self.name}: cleanup ok")
        except Exception as exc:  # noqa: BLE001
            cleanup_error = exc

        if run_error and cleanup_error:
            raise RuntimeError(
                f"{self.name}: run failed ({run_error}); cleanup failed ({cleanup_error})"
            ) from run_error
        if run_error:
            raise run_error
        if cleanup_error:
            raise cleanup_error


class ProjectRunTestCase(E2ETestCase):
    def __init__(
        self,
        name: str,
        test_dir: Path,
        run_args: Sequence[str],
        retrieve_paths: Sequence[str],
    ):
        super().__init__(name, test_dir)
        self.run_args = list(run_args)
        self.retrieve_paths = list(retrieve_paths)
        self.job_ids: List[int] = []
        self.job_paths: Dict[int, Optional[str]] = {}
        self.primary_job_id: Optional[int] = None

    def submit_job(
        self,
        ctx: E2EContext,
        *,
        extra_args: Optional[Sequence[str]] = None,
        non_interactive: bool = False,
    ):
        cmd = ctx.build_project_run_cmd(
            self.test_dir,
            self.run_args,
            extra_args=extra_args,
            non_interactive=non_interactive,
        )
        result = ctx.run(cmd)
        if non_interactive:
            job_id, remote_path = parse_run_json(result)
        else:
            job_id, remote_path = parse_run_result(result)
        self.job_ids.append(job_id)
        self.job_paths[job_id] = remote_path
        if self.primary_job_id is None:
            self.primary_job_id = job_id
        print(f"{self.name}: submitted job {job_id}")
        return job_id, remote_path

    def setup(self, ctx: E2EContext):
        self.submit_job(ctx)
        self.setup_after_primary_submit(ctx)

    def setup_after_primary_submit(self, ctx: E2EContext):
        pass

    def run(self, ctx: E2EContext):
        for active_job_id in self.job_ids:
            wait_for_job(ctx.orbit_cmd, active_job_id, ctx.timeout_secs, ctx.poll_secs)
            wait_for_job_list_entry(
                ctx.non_interactive_cmd,
                ctx.cluster,
                active_job_id,
                ctx.timeout_secs,
                ctx.poll_secs,
            )
            validate_directory_job_has_no_blueprint(
                ctx.non_interactive_cmd,
                ctx.cluster,
                active_job_id,
            )
            print(f"{self.name}: job {active_job_id} completed")

        self.after_jobs_complete(ctx)

        if self.primary_job_id is None:
            raise RuntimeError(f"{self.name}: missing primary job")

        project_out = ctx.out_dir / self.name / str(self.primary_job_id)
        project_out.mkdir(parents=True, exist_ok=True)

        for path in self.retrieve_paths:
            retrieve_cmd = ctx.orbit_cmd + [
                "job",
                "retrieve",
                str(self.primary_job_id),
                path,
                "--output",
                str(project_out),
                "--overwrite",
            ]
            ctx.run(retrieve_cmd)

        self.validate(project_out, ctx)
        print(f"{self.name}: validation ok")

    def after_jobs_complete(self, ctx: E2EContext):
        pass

    def validate(self, project_out: Path, ctx: E2EContext):
        pass

    def cleanup(self, ctx: E2EContext):
        for cleanup_job_id in self.job_ids:
            cleanup_job_and_validate(
                ctx.orbit_cmd,
                ctx.cluster,
                cleanup_job_id,
                self.job_paths.get(cleanup_job_id),
            )
            print(f"{self.name}: cleanup ok for job {cleanup_job_id}")


class ClusterInterfaceTest(E2ETestCase):
    def run(self, ctx: E2EContext):
        validate_cluster_list_flags(ctx.non_interactive_cmd, ctx.cluster)
        print("cluster list flags: ok")
        validate_cluster_set_key_value(ctx.non_interactive_cmd, ctx.cluster)
        print("cluster set key=value: ok")


class SmokeProjectTest(ProjectRunTestCase):
    def __init__(self, test_dir: Path):
        super().__init__(
            "01_smoke",
            test_dir,
            run_args=[],
            retrieve_paths=["results"],
        )

    def setup_after_primary_submit(self, ctx: E2EContext):
        if not self.job_ids:
            raise RuntimeError("01_smoke: missing primary job id")
        primary_job_id = self.job_ids[0]
        primary_remote_path = self.job_paths[primary_job_id]

        conflict_cmd = ctx.build_project_run_cmd(
            self.test_dir,
            self.run_args,
            non_interactive=True,
        )
        conflict_status, conflict_output = ctx.run_status(conflict_cmd)
        if conflict_status == 0:
            raise RuntimeError("run should fail while a job is running in the same directory")
        conflict_error = parse_orbit_error_output(conflict_output)
        validate_run_conflict_error(conflict_error, expected_remote_path=primary_remote_path)

        new_dir_job_id, new_dir_remote_path = self.submit_job(
            ctx,
            extra_args=["--new-directory"],
        )
        if new_dir_remote_path == primary_remote_path:
            raise RuntimeError("new-directory run did not create a new remote path")
        self.primary_job_id = new_dir_job_id

    def after_jobs_complete(self, ctx: E2EContext):
        if self.primary_job_id is None:
            raise RuntimeError("01_smoke: missing primary job for log validation")
        validate_smoke_logs(ctx.orbit_cmd, self.primary_job_id)
        print("01_smoke: logs ok")

    def validate(self, project_out: Path, ctx: E2EContext):
        validate_smoke(project_out, ctx.repo_root)


class PythonStatsTest(ProjectRunTestCase):
    def __init__(self, test_dir: Path):
        super().__init__(
            "02_python_stats",
            test_dir,
            run_args=["--sbatchscript", "scripts/submit.sbatch"],
            retrieve_paths=["results"],
        )

    def validate(self, project_out: Path, _ctx: E2EContext):
        validate_python_stats(project_out)


class FilterTreeTest(ProjectRunTestCase):
    def __init__(self, test_dir: Path):
        super().__init__(
            "03_filter_tree",
            test_dir,
            run_args=[
                "--include",
                "cache/keep.txt",
                "--exclude",
                "cache/*",
                "--exclude",
                "*.tmp",
                "--exclude",
                "build/",
            ],
            retrieve_paths=["results/files.txt"],
        )

    def validate(self, project_out: Path, _ctx: E2EContext):
        validate_filter_tree(project_out)


class BinaryOutputTest(ProjectRunTestCase):
    def __init__(self, test_dir: Path):
        super().__init__(
            "04_binary_output",
            test_dir,
            run_args=[],
            retrieve_paths=[
                "results/raw/random.bin",
                "results/raw/random.sha256",
                "results/text",
            ],
        )

    def validate(self, project_out: Path, ctx: E2EContext):
        validate_binary_output(project_out, ctx.repo_root)


class CancelJobTest(ProjectRunTestCase):
    def __init__(self, test_dir: Path):
        super().__init__(
            "05_cancel_job",
            test_dir,
            run_args=[],
            retrieve_paths=[],
        )

    def run(self, ctx: E2EContext):
        if self.primary_job_id is None:
            raise RuntimeError("05_cancel_job: missing job id")
        cancel_cmd = ctx.orbit_cmd + ["job", "cancel", str(self.primary_job_id), "--yes"]
        ctx.run(cancel_cmd)
        wait_for_job_canceled(ctx.orbit_cmd, self.primary_job_id, ctx.timeout_secs, ctx.poll_secs)
        print(f"05_cancel_job: job {self.primary_job_id} canceled")


class TemplatedHashesTest(ProjectRunTestCase):
    def __init__(self, test_dir: Path):
        super().__init__(
            "06_templated_hashes",
            test_dir,
            run_args=[
                "--field",
                "hash_method=sha256",
                "--field",
                "file_count=3",
                "--fill-defaults",
            ],
            retrieve_paths=["results"],
        )

    def validate(self, project_out: Path, _ctx: E2EContext):
        validate_templated_hashes(project_out, 3, "sha256")


class BlueprintLifecycleTest(E2ETestCase):
    def __init__(self, test_dir: Path, out_dir: Path):
        super().__init__("blueprint_lifecycle", test_dir)
        self.workspace_dir = out_dir / "_blueprint_lifecycle_src"
        self.blueprint_name: Optional[str] = None
        self.blueprint_ref: Optional[str] = None
        self.blueprint_version: Optional[str] = None
        self.job_id: Optional[int] = None
        self.remote_path: Optional[str] = None

    def setup(self, ctx: E2EContext):
        if self.workspace_dir.exists():
            shutil.rmtree(self.workspace_dir)
        shutil.copytree(self.test_dir, self.workspace_dir)

        self.blueprint_name = f"e2e_blueprint_{uuid.uuid4().hex[:10]}"
        ctx.run(
            ctx.orbit_cmd
            + [
                "init",
                str(self.workspace_dir),
                "--name",
                self.blueprint_name,
            ]
        )

        listed = parse_json_output(ctx.run(ctx.non_interactive_cmd + ["blueprint", "list"]))
        blueprints_payload = listed.get("blueprints") or []
        if any(item.get("name") == self.blueprint_name for item in blueprints_payload):
            raise RuntimeError("blueprint lifecycle: init should not register blueprint")

        build_result = parse_json_output(
            ctx.run(ctx.non_interactive_cmd + ["blueprint", "build", str(self.workspace_dir)])
        )
        self.blueprint_version = build_result.get("versionTag")
        if build_result.get("name") != self.blueprint_name or not self.blueprint_version:
            raise RuntimeError("blueprint lifecycle: build returned unexpected metadata")
        self.blueprint_ref = f"{self.blueprint_name}:{self.blueprint_version}"

        listed = parse_json_output(ctx.run(ctx.non_interactive_cmd + ["blueprint", "list"]))
        blueprints_payload = listed.get("blueprints") or []
        blueprint_record = next(
            (
                item
                for item in blueprints_payload
                if item.get("name") == self.blueprint_name
                and self.blueprint_version in (item.get("tags") or [])
            ),
            None,
        )
        if blueprint_record is None:
            raise RuntimeError("blueprint lifecycle: built blueprint missing from blueprint list")
        if blueprint_record.get("latest_tag") != self.blueprint_version:
            raise RuntimeError(
                "blueprint lifecycle: latest_tag did not match built version "
                f"(expected={self.blueprint_version}, actual={blueprint_record.get('latest_tag')})"
            )

    def run(self, ctx: E2EContext):
        if not self.blueprint_ref or not self.blueprint_name:
            raise RuntimeError("blueprint lifecycle: missing blueprint reference")

        blueprint_run = ctx.run(
            ctx.non_interactive_cmd
            + [
                "blueprint",
                "run",
                self.blueprint_ref,
                "--on",
                ctx.cluster,
            ]
        )
        self.job_id, self.remote_path = parse_run_json(blueprint_run)
        print(f"blueprint lifecycle: submitted job {self.job_id}")

        status, terminal_state = job_status(ctx.orbit_cmd, self.job_id)
        if status not in ("queued", "running", "completed"):
            raise RuntimeError(
                "blueprint lifecycle: unexpected initial job status "
                f"{status} (terminal_state={terminal_state})"
            )

        wait_for_job(ctx.orbit_cmd, self.job_id, ctx.timeout_secs, ctx.poll_secs)
        status, terminal_state = job_status(ctx.orbit_cmd, self.job_id)
        if status != "completed":
            raise RuntimeError(
                "blueprint lifecycle: expected completed status, got "
                f"{status} (terminal_state={terminal_state})"
            )

        validate_blueprint_job_filter(
            ctx.non_interactive_cmd,
            ctx.cluster,
            self.blueprint_name,
            self.job_id,
        )
        print("blueprint lifecycle: job list --blueprint filter ok")

    def cleanup(self, ctx: E2EContext):
        if self.job_id is not None and self.remote_path:
            cleanup_job_and_validate(ctx.orbit_cmd, ctx.cluster, self.job_id, self.remote_path)
            print(f"blueprint lifecycle: cleanup ok for job {self.job_id}")

        if self.blueprint_ref and self.blueprint_name:
            ctx.run(ctx.orbit_cmd + ["blueprint", "delete", self.blueprint_ref, "--yes"])
            listed_after_delete = parse_json_output(
                ctx.run(ctx.non_interactive_cmd + ["blueprint", "list"])
            )
            blueprints_after_delete = listed_after_delete.get("blueprints") or []
            if any(item.get("name") == self.blueprint_name for item in blueprints_after_delete):
                raise RuntimeError("blueprint lifecycle: blueprint still present after delete")
            print(f"blueprint lifecycle: deleted blueprint {self.blueprint_name}")

        if self.workspace_dir.exists():
            shutil.rmtree(self.workspace_dir)


class NonInteractiveSmokeTest(E2ETestCase):
    def __init__(self, test_dir: Path):
        super().__init__("non_interactive", test_dir)
        self.job_id: Optional[int] = None
        self.remote_path: Optional[str] = None

    def setup(self, ctx: E2EContext):
        ping_result = parse_json_output(ctx.run(ctx.non_interactive_cmd + ["ping"]))
        if ping_result.get("message") != "pong":
            raise RuntimeError("non-interactive ping returned unexpected response")

        run_result = ctx.run(
            ctx.build_project_run_cmd(
                self.test_dir,
                [],
                non_interactive=True,
            )
        )
        self.job_id, self.remote_path = parse_run_json(run_result)
        if not self.remote_path:
            raise RuntimeError("non-interactive run missing remote_path")
        print(f"non-interactive: submitted job {self.job_id}")

    def run(self, ctx: E2EContext):
        if self.job_id is None:
            raise RuntimeError("non-interactive: missing job id")
        wait_for_job(ctx.non_interactive_cmd, self.job_id, ctx.timeout_secs, ctx.poll_secs)

        validate_smoke_logs_json(ctx.non_interactive_cmd, self.job_id)
        print("non-interactive: logs ok")

        non_interactive_out = ctx.out_dir / "non_interactive" / str(self.job_id)
        non_interactive_out.mkdir(parents=True, exist_ok=True)
        retrieve_cmd = ctx.non_interactive_cmd + [
            "job",
            "retrieve",
            str(self.job_id),
            "results",
            "--output",
            str(non_interactive_out),
            "--overwrite",
        ]
        ctx.run(retrieve_cmd)

        validate_smoke(non_interactive_out, ctx.repo_root)
        print("non-interactive: validation ok")

    def cleanup(self, ctx: E2EContext):
        if self.job_id is not None:
            cleanup_job_and_validate(ctx.orbit_cmd, ctx.cluster, self.job_id, self.remote_path)
            print(f"non-interactive: cleanup ok for job {self.job_id}")


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

    ctx = E2EContext(
        repo_root=repo_root,
        out_dir=out_dir,
        cluster=args.cluster,
        timeout_secs=args.timeout,
        poll_secs=args.poll,
        orbit_cmd=orbit_cmd,
        non_interactive_cmd=non_interactive_cmd,
    )

    tests: List[E2ETestCase] = [
        ClusterInterfaceTest("cluster_interface", repo_root / "tests"),
        SmokeProjectTest(repo_root / "tests/01_smoke"),
        PythonStatsTest(repo_root / "tests/02_python_stats"),
        FilterTreeTest(repo_root / "tests/03_filter_tree"),
        BinaryOutputTest(repo_root / "tests/04_binary_output"),
        CancelJobTest(repo_root / "tests/05_cancel_job"),
        TemplatedHashesTest(repo_root / "tests/06_templated_hashes"),
        BlueprintLifecycleTest(repo_root / "tests/93_hello_world", out_dir),
        NonInteractiveSmokeTest(repo_root / "tests/01_smoke"),
    ]

    daemon_proc = None
    try:
        daemon_proc = start_daemon(
            orbitd_cmd,
            orbit_cmd + ["ping"],
            args.daemon_timeout,
            args.poll,
        )
        run_cmd(orbit_cmd + ["ping"])

        for test_case in tests:
            test_case.execute(ctx)

        print("All scenarios completed.")
        return 0
    finally:
        stop_daemon(daemon_proc)
        if not args.keep:
            shutil.rmtree(out_dir, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
