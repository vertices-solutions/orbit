---
title: "orbit blueprint run"
description: "Run a registered blueprint by blueprint name"
---

Run a registered blueprint by blueprint name

Parent command: [`orbit blueprint`](./orbit-blueprint)

**Usage:** `orbit blueprint run [OPTIONS] <BLUEPRINT>`

## Arguments

- `<BLUEPRINT>`: Built blueprint name:tag (e.g., my-blueprint:20260112.001 or my-blueprint:latest)

## Flags and Options

- `--on <CLUSTER>`: Cluster name
- `--non-interactive`: Run without prompts, fail on MFA, and output JSON only.
- `--sbatchscript <PATH>`: Path to the sbatch script to run
- `--preset <PRESET>`: Apply a template preset before prompting for missing fields
- `--field <KEY=VALUE>`: Template field override in KEY=VALUE form (repeatable)
- `--fill-defaults`: Accept default template values without prompting (interactive mode only)
- `--remote-path <REMOTE_PATH>`
- `--new-directory`: Always create a new remote directory, even if this local path was run before
- `--force`: Allow running into a remote directory even if another job is running there
- `--include <PATTERN>`: Include paths matching PATTERN
- `--exclude <PATTERN>`: Exclude paths matching PATTERN
- `-h, --help`: Print help