---
title: "orbit job ls"
description: "List files in a job work directory"
---

List files in a job work directory

Parent command: [`orbit job`](./orbit-job)

**Usage:** `orbit job ls [OPTIONS] <JOB_ID> [PATH]`

## Arguments

- `<JOB_ID>`: Job id from the daemon
- `[PATH]`: Path to list (absolute or relative to the job root)

## Flags and Options

- `--cluster <CLUSTER>`
- `--non-interactive`: Run without prompts, fail on MFA, and output JSON only.
- `-h, --help`: Print help