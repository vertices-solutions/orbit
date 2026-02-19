---
title: "orbit"
description: "Top-level help for the orbit CLI"
---

**Usage:** `orbit [OPTIONS] <COMMAND>`

## Flags and Options

- `-c, --config <PATH>`: Path to a TOML config file. When omitted, orbit uses ORBIT_CONFIG_PATH if set, otherwise the default config file location if available.
- `--non-interactive`: Run without prompts, fail on MFA, and output JSON only.
- `-h, --help`: Print help
- `-V, --version`: Print version

## Subcommands

- [`orbit ping`](./orbit-ping): Check that the daemon is reachable
- [`orbit init`](./orbit-init): Initialize a project directory with an Orbitfile
- [`orbit run`](./orbit-run): Run either a local project directory or a versioned blueprint
- [`orbit job`](./job/orbit-job): Operations on jobs: run jobs, inspect status, and retrieve outputs/results
- [`orbit cluster`](./cluster/orbit-cluster): Operations on clusters: add, connect, delete, poll, and manage clusters
- [`orbit blueprint`](./blueprint/orbit-blueprint): Operations on registered blueprints
- [`orbit completions`](./orbit-completions): Generate shell completions