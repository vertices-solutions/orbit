---
title: "orbit cluster"
description: "Operations on clusters: add, connect, delete, poll, and manage clusters"
---

Operations on clusters: add, connect, delete, poll, and manage clusters

Parent command: [`orbit`](../orbit)

**Usage:** `orbit cluster [OPTIONS] <COMMAND>`

## Flags and Options

- `--non-interactive`: Run without prompts, fail on MFA, and output JSON only.
- `-h, --help`: Print help

## Subcommands

- [`orbit cluster list`](./orbit-cluster-list): List clusters
- [`orbit cluster get`](./orbit-cluster-get): Show cluster details
- [`orbit cluster ls`](./orbit-cluster-ls): List files on a cluster
- [`orbit cluster add`](./orbit-cluster-add): Add a new cluster
- [`orbit cluster set`](./orbit-cluster-set): Update cluster parameters
- [`orbit cluster connect`](./orbit-cluster-connect): Connect to a cluster and validate the SSH session
- [`orbit cluster delete`](./orbit-cluster-delete): Delete a cluster and its job records
