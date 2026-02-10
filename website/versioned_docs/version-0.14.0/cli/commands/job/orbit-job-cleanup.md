---
title: "orbit job cleanup"
description: "Clean up a job's remote directory"
---

Clean up a job's remote directory

Parent command: [`orbit job`](./orbit-job)

**Usage:** `orbit job cleanup [OPTIONS] <JOB_ID>`

## Arguments

- `<JOB_ID>`: Job id from the daemon

## Flags and Options

- `--force`: Cancel a running job before cleanup
- `--full`: Delete the job record from the local database after cleanup
- `--non-interactive`: Run without prompts, fail on MFA, and output JSON only.
- `-y, --yes`: Skip the confirmation prompt
- `-h, --help`: Print help