---
title: "orbit blueprint"
description: "Operations on registered blueprints"
---

Operations on registered blueprints

Parent command: [`orbit`](../orbit)

**Usage:** `orbit blueprint [OPTIONS] <COMMAND>`

## Flags and Options

- `--non-interactive`: Run without prompts, fail on MFA, and output JSON only.
- `-h, --help`: Print help

## Subcommands

- [`orbit blueprint build`](./orbit-blueprint-build): Build a blueprint tarball and register it locally
- [`orbit blueprint run`](./orbit-blueprint-run): Run a registered blueprint by blueprint name
- [`orbit blueprint list`](./orbit-blueprint-list): List registered blueprints
- [`orbit blueprint delete`](./orbit-blueprint-delete): Delete a blueprint from the local registry