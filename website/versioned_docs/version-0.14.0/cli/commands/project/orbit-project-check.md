---
title: "orbit project check"
description: "Validate one or all registered projects. `orbit project check` validates local registry entries against project roots on disk. For each selected project, it verifies that the registered path exists and is a directory, that an Orbitfile exists at the project root and can be parsed, that `[project].name` in Orbitfile matches the registered project name, and that `[submit].sbatch_script` (if set) resolves to a file inside the project root."
---

Validate one or all registered projects. `orbit project check` validates local registry entries against project roots on disk. For each selected project, it verifies that the registered path exists and is a directory, that an Orbitfile exists at the project root and can be parsed, that `[project].name` in Orbitfile matches the registered project name, and that `[submit].sbatch_script` (if set) resolves to a file inside the project root.

Parent command: [`orbit project`](./orbit-project)

**Usage:** `orbit project check [OPTIONS] [NAME]`

## Arguments

- `[NAME]`: Optional project identifier (`name` or `name:tag`).
- `If omitted, `orbit project check` validates all registered projects.`

## Flags and Options

- `--non-interactive`: Run without prompts, fail on MFA, and output JSON only.
- `-h, --help`: Print help (see a summary with '-h')