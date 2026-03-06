---
title: "orbit job retrieve"
description: "Retrieve a file or directory from a job run folder"
---

Retrieve a file or directory from a job run folder

Parent command: [`orbit job`](./orbit-job)

**Usage:** `orbit job retrieve [OPTIONS] <JOB_ID> [PATH]`

## Arguments

- `<JOB_ID>`: Job id from the daemon
- `[PATH]`: Optional remote path (absolute or relative to the job run folder)

## Flags and Options

- `--output <OUTPUT>`: Directory where the requested file or directory will be placed.
- `--non-interactive`: Run without prompts, fail on MFA, and output JSON only.
- `--overwrite`: Overwrite existing local files.
- `--force`: Retrieve outputs even if the job has not completed.
- `-h, --help`: Print help