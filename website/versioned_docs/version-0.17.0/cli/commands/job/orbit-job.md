---
title: "orbit job"
description: "Operations on jobs: run jobs, inspect status, and retrieve outputs/results"
---

Operations on jobs: run jobs, inspect status, and retrieve outputs/results

Parent command: [`orbit`](../orbit)

**Usage:** `orbit job [OPTIONS] <COMMAND>`

## Flags and Options

- `--non-interactive`: Run without prompts, fail on MFA, and output JSON only.
- `-h, --help`: Print help

## Subcommands

- [`orbit job run`](./orbit-job-run): Run a local directory on a cluster
- [`orbit job list`](./orbit-job-list): List jobs
- [`orbit job get`](./orbit-job-get): Show job details
- [`orbit job logs`](./orbit-job-logs): Show job logs
- [`orbit job cancel`](./orbit-job-cancel): Cancel a job
- [`orbit job cleanup`](./orbit-job-cleanup): Clean up a job's remote directory
- [`orbit job ls`](./orbit-job-ls): List files in a job work directory
- [`orbit job retrieve`](./orbit-job-retrieve): Retrieve a file or directory from a job run folder