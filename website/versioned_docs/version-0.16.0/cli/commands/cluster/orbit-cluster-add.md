---
title: "orbit cluster add"
description: "Add a new cluster"
---

Add a new cluster

Parent command: [`orbit cluster`](./orbit-cluster)

**Usage:** `orbit cluster add [OPTIONS] [DESTINATION]`

## Arguments

- `[DESTINATION]`: Destination in ssh format: user@host[:port] (required in non-interactive mode)

## Flags and Options

- `--name <NAME>`: Friendly cluster name you’ll use in other commands (e.g. "gpu01" or "lab-cluster")
- `--identity-path <IDENTITY_PATH>`: In interactive mode, pick from discovered ~/.ssh keys (or enter a custom path via "other"). In non-interactive mode, defaults to the first discovered key (prefers ed25519, then rsa)
- `--non-interactive`: Run without prompts, fail on MFA, and output JSON only.
- `--default-base-path <DEFAULT_BASE_PATH>`
- `--default`: Mark this cluster as the default cluster
- `-h, --help`: Print help