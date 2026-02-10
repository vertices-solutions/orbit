---
title: "orbit project submit"
description: "Submit a registered project by project name"
---

Submit a registered project by project name

Parent command: [`orbit project`](./orbit-project)

**Usage:** `orbit project submit [OPTIONS] --to <CLUSTER> <PROJECT>`

## Arguments

- `<PROJECT>`: Built project name:tag (e.g., my-project:20260112.001 or my-project:latest)

## Flags and Options

- `--to <CLUSTER>`: Cluster name
- `--non-interactive`: Run without prompts, fail on MFA, and output JSON only.
- `--sbatchscript <PATH>`: Path to the sbatch script to submit
- `--preset <PRESET>`: Apply a template preset before prompting for missing fields
- `--field <KEY=VALUE>`: Template field override in KEY=VALUE form (repeatable)
- `--fill-defaults`: Accept default template values without prompting (interactive mode only)
- `--remote-path <REMOTE_PATH>`
- `--new-directory`: Always create a new remote directory, even if this local path was submitted before
- `--force`: Allow submitting into a remote directory even if another job is running there
- `--include <PATTERN>`: Include paths matching PATTERN
- `--exclude <PATTERN>`: Exclude paths matching PATTERN
- `-h, --help`: Print help