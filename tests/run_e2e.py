#!/usr/bin/env python3
import argparse
import hashlib
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import time
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
    try:
        if os.name == "nt":
            proc.terminate()
        else:
            os.killpg(proc.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        proc.wait(timeout=timeout_secs)
    except subprocess.TimeoutExpired:
        if os.name == "nt":
            proc.kill()
        else:
            os.killpg(proc.pid, signal.SIGKILL)
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
    raise RuntimeError("unable to parse job id from submit output")


def parse_remote_path(output):
    cleaned = strip_ansi(output)
    for line in cleaned.splitlines():
        if "Remote path:" in line:
            _, _, path = line.partition("Remote path:")
            path = path.strip()
            if path:
                return path
    raise RuntimeError("unable to parse remote path from submit output")


def parse_orbit_json(output):
    data = json.loads(output)
    if isinstance(data, dict) and "ok" in data:
        if not data.get("ok"):
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


def parse_submit_json(result):
    data = parse_json_output(result)
    job_id = data.get("job_id")
    if job_id is None:
        raise RuntimeError("submit JSON missing job_id")
    return job_id, data.get("remote_path")


def combined_stream_text(data):
    return (data.get("stdout") or "") + (data.get("stderr") or "")


def job_status(orbit_cmd, job_id):
    result = run_cmd(orbit_cmd + ["job", "get", str(job_id), "--json"])
    data = parse_json_output(result)
    return data.get("status"), data.get("terminal_state")


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


def build_submit_cmd(orbit_cmd, cluster, project_path, submit_args, headless, extra_args=None):
    cmd = orbit_cmd + [
        "job",
        "submit",
        cluster,
        str(project_path),
    ]
    cmd.extend(submit_args)
    if extra_args:
        cmd.extend(extra_args)
    if headless:
        cmd.append("--headless")
    return cmd


def main():
    parser = argparse.ArgumentParser(description="Run orbit end-to-end test projects.")
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
    parser.add_argument("--headless", action="store_true", help="Use headless mode.")
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    config_path = Path(args.config)
    if not config_path.is_absolute():
        config_path = (repo_root / config_path).resolve()
    if not config_path.is_file():
        raise RuntimeError(f"config file not found: {config_path}")
    orbit_cmd = command_with_config(args.orbit_bin, config_path)
    orbitd_cmd = command_with_config(args.orbitd_bin, config_path)
    non_interactive_cmd = orbit_cmd + ["--non-interactive"]
    out_dir = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
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

        projects = [
            {
                "id": "01_smoke",
                "path": repo_root / "tests/01_smoke",
                "submit_args": [],
                "retrieve": ["results"],
                "validate": lambda out: validate_smoke(out, repo_root),
            },
            {
                "id": "02_python_stats",
                "path": repo_root / "tests/02_python_stats",
                "submit_args": ["scripts/submit.sbatch"],
                "retrieve": ["results"],
                "validate": validate_python_stats,
            },
            {
                "id": "03_filter_tree",
                "path": repo_root / "tests/03_filter_tree",
            "submit_args": [
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
                "submit_args": [],
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
                "submit_args": [],
                "cancel": True,
                "retrieve": [],
                "validate": lambda out: None,
            },
        ]

        for project in projects:
            submit_cmd = build_submit_cmd(
                orbit_cmd,
                args.cluster,
                project["path"],
                project["submit_args"],
                args.headless,
            )
            result = run_cmd(submit_cmd)
            output = result.stdout + result.stderr
            job_id = parse_job_id(output)
            job_ids = [job_id]
            primary_job_id = job_id
            if project.get("cancel"):
                print(f"{project['id']}: submitted job {job_id}")
                cancel_cmd = orbit_cmd + ["job", "cancel", str(job_id), "--yes"]
                run_cmd(cancel_cmd)
                wait_for_job_canceled(orbit_cmd, job_id, args.timeout, args.poll)
                print(f"{project['id']}: job {job_id} canceled")
                continue
            if project["id"] == "01_smoke":
                remote_path = parse_remote_path(output)
                conflict_status, conflict_output = run_cmd_status(submit_cmd)
                if conflict_status == 0:
                    raise RuntimeError(
                        "submit should fail while a job is running in the same directory"
                    )
                expected = f"job {job_id} is still running"
                if expected not in conflict_output:
                    raise RuntimeError(
                        "submit conflict message missing running job id"
                    )

                force_cmd = build_submit_cmd(
                    orbit_cmd,
                    args.cluster,
                    project["path"],
                    project["submit_args"],
                    args.headless,
                    extra_args=["--force"],
                )
                force_result = run_cmd(force_cmd)
                force_output = force_result.stdout + force_result.stderr
                force_job_id = parse_job_id(force_output)
                force_remote_path = parse_remote_path(force_output)
                if force_remote_path != remote_path:
                    raise RuntimeError(
                        "force submit did not reuse existing remote path"
                    )
                job_ids.append(force_job_id)

                new_dir_cmd = build_submit_cmd(
                    orbit_cmd,
                    args.cluster,
                    project["path"],
                    project["submit_args"],
                    args.headless,
                    extra_args=["--new-directory"],
                )
                new_dir_result = run_cmd(new_dir_cmd)
                new_dir_output = new_dir_result.stdout + new_dir_result.stderr
                new_dir_job_id = parse_job_id(new_dir_output)
                new_dir_remote_path = parse_remote_path(new_dir_output)
                if new_dir_remote_path == remote_path:
                    raise RuntimeError(
                        "new-directory submit did not create a new remote path"
                    )
                job_ids.append(new_dir_job_id)
                primary_job_id = new_dir_job_id

            for active_job_id in job_ids:
                print(f"{project['id']}: submitted job {active_job_id}")
                wait_for_job(orbit_cmd, active_job_id, args.timeout, args.poll)
                print(f"{project['id']}: job {active_job_id} completed")

            if project["id"] == "01_smoke":
                validate_smoke_logs(orbit_cmd, primary_job_id)
                print(f"{project['id']}: logs ok")

            project_out = out_dir / project["id"] / str(primary_job_id)
            project_out.mkdir(parents=True, exist_ok=True)

            for path in project["retrieve"]:
                retrieve_cmd = orbit_cmd + [
                    "job",
                    "retrieve",
                    str(primary_job_id),
                    path,
                    "--output",
                    str(project_out),
                    "--overwrite",
                ]
                if args.headless:
                    retrieve_cmd.append("--headless")
                run_cmd(retrieve_cmd)

            project["validate"](project_out)
            print(f"{project['id']}: validation ok")

        ping_result = parse_json_output(run_cmd(non_interactive_cmd + ["ping"]))
        if ping_result.get("message") != "pong":
            raise RuntimeError("non-interactive ping returned unexpected response")

        non_interactive_project = repo_root / "tests/01_smoke"
        submit_cmd = build_submit_cmd(
            non_interactive_cmd,
            args.cluster,
            non_interactive_project,
            [],
            args.headless,
        )
        submit_result = run_cmd(submit_cmd)
        job_id, remote_path = parse_submit_json(submit_result)
        if not remote_path:
            raise RuntimeError("non-interactive submit missing remote_path")
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

        print("All projects completed.")
        return 0
    finally:
        stop_daemon(daemon_proc)
        if not keep_outputs:
            shutil.rmtree(out_dir, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
