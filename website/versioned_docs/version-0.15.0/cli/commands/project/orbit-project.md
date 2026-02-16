---
title: "orbit project"
description: "Operations on local projects and Orbitfile metadata"
---

Operations on local projects and Orbitfile metadata

Parent command: [`orbit`](../orbit)

**Usage:** `orbit project [OPTIONS] <COMMAND>`

## Flags and Options

- `--non-interactive`: Run without prompts, fail on MFA, and output JSON only.
- `-h, --help`: Print help

## Subcommands

- [`orbit project init`](./orbit-project-init): Initialize a project root and Orbitfile
- [`orbit project build`](./orbit-project-build): Build a project tarball and register it locally
- [`orbit project submit`](./orbit-project-submit): Submit a registered project by project name
- [`orbit project list`](./orbit-project-list): List registered projects
- [`orbit project delete`](./orbit-project-delete): Delete a project from the local registry