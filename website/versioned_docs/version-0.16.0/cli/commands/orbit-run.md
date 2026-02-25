---
title: "orbit run"
description: "Run either a local project directory or a versioned blueprint"
---

Run either a local project directory or a versioned blueprint

Parent command: [`orbit`](./orbit)

**Usage:** `orbit run [OPTIONS] <TARGET>`

## Arguments

- `<TARGET>`: Path to a project you want to run on cluster - either in local directory or blueprint in `<name:tag>` format. Orbit resolves directories first; if not a directory, it resolves as a blueprint

## Flags and Options

- `--on <CLUSTER>`: Cluster name. If omitted, the default cluster will be used
- `--non-interactive`: Run without prompts, fail on MFA, and output JSON only.
- `--sbatchscript <PATH>`: Path to the sbatch script to run
- `--preset <PRESET>`: Apply a template preset before prompting for missing fields
- `--field <KEY=VALUE>`: Template field override in KEY=VALUE form (repeatable)
- `--fill-defaults`: Accept default template values without prompting (interactive mode only)
- `--remote-path <REMOTE_PATH>`
- `--new-directory`: Always create a new remote directory, even if this local path was run before
- `--include <PATTERN>`: Include paths matching PATTERN
- `--exclude <PATTERN>`: Exclude paths matching PATTERN
- `-h, --help`: Print help