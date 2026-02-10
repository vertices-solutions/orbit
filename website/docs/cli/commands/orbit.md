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
- [`orbit job`](./job/orbit-job): Operations on jobs: submit job, inspect its status, and retrieve its outputs and results
- [`orbit cluster`](./cluster/orbit-cluster): Operations on clusters: add, delete, poll, and manage clusters
- [`orbit project`](./project/orbit-project): Operations on local projects and Orbitfile metadata
- [`orbit completions`](./orbit-completions): Generate shell completions