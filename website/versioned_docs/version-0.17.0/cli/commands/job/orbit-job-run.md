---
title: "orbit job run"
description: "Run a local directory on a cluster"
---

Run a local directory on a cluster

Parent command: [`orbit job`](./orbit-job)

**Usage:** `orbit job run [OPTIONS] <LOCAL_PATH>`

## Arguments

- `<LOCAL_PATH>`

## Flags and Options

- `--on <CLUSTER>`: Cluster name
- `--non-interactive`: Run without prompts, fail on MFA, and output JSON only.
- `--sbatchscript <PATH>`: Path to the sbatch script to run
- `--preset <PRESET>`: Apply a template preset before prompting for missing fields
- `--field <KEY=VALUE>`: Template field override in KEY=VALUE form (repeatable)
- `--fill-defaults`: Accept default template values without prompting (interactive mode only)
- `--remote-path <REMOTE_PATH>`
- `--new-directory`: Always create a new remote directory, even if this local path was run before
- `--include <PATTERN>`: Include paths matching PATTERN. Rules are checked in the order they appear across --include/--exclude; the first match wins, and unmatched paths are included. Patterns match the path relative to the run root with '/' separators. A pattern without '/' matches the basename anywhere; a pattern with '/' but no leading '/' is treated as `**/PATTERN`. A leading '/' anchors to the root, and a trailing '/' matches directories only (and prunes their contents). Globs support `*`, `?`, `**`, `[]`, `{}`
- `--exclude <PATTERN>`: Exclude paths matching PATTERN. Rules are checked in the order they appear across --include/--exclude; the first match wins, and unmatched paths are included. Patterns match the path relative to the run root with '/' separators. A pattern without '/' matches the basename anywhere; a pattern with '/' but no leading '/' is treated as `**/PATTERN`. A leading '/' anchors to the root, and a trailing '/' matches directories only (and prunes their contents). Globs support `*`, `?`, `**`, `[]`, `{}`
- `-h, --help`: Print help