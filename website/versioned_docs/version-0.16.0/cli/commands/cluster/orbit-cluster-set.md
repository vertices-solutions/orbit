---
title: "orbit cluster set"
description: "Update cluster parameters"
---

Update cluster parameters

Parent command: [`orbit cluster`](./orbit-cluster)

**Usage:** `orbit cluster set [OPTIONS] --on <CLUSTER> <KEY=VALUE>`

## Arguments

- `<KEY=VALUE>`: Single setting assignment in KEY=VALUE form. Supported keys: host, username, port, identity_path, default_base_path, default

## Flags and Options

- `--on <CLUSTER>`: Cluster name to update
- `--non-interactive`: Run without prompts, fail on MFA, and output JSON only.
- `-h, --help`: Print help