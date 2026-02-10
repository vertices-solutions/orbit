---
title: "orbit job submit"
description: "Submit a project to a cluster"
---

Submit a project to a cluster

Parent command: [`orbit job`](./orbit-job)

**Usage:** `orbit job submit [OPTIONS] --to <CLUSTER> <LOCAL_PATH>`

## Arguments

- `<LOCAL_PATH>`

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
- `--include <PATTERN>`: Include paths matching PATTERN. Rules are checked in the order they appear across
- `--include/--exclude; the first match wins, and unmatched paths are included. Patterns match the path`: relative to the submit root with '/' separators. A pattern without '/' matches the basename anywhere; a pattern with '/' but no leading '/' is treated as `**/PATTERN`. A leading '/' anchors to the root, and a trailing '/' matches directories only (and prunes their contents). Globs support `*`, `?`, `**`, `[]`, `{}`
- `--exclude <PATTERN>`: Exclude paths matching PATTERN. Rules are checked in the order they appear across
- `--include/--exclude; the first match wins, and unmatched paths are included. Patterns match the path`: relative to the submit root with '/' separators. A pattern without '/' matches the basename anywhere; a pattern with '/' but no leading '/' is treated as `**/PATTERN`. A leading '/' anchors to the root, and a trailing '/' matches directories only (and prunes their contents). Globs support `*`, `?`, `**`, `[]`, `{}`
- `-h, --help`: Print help