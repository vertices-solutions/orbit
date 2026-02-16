---
title: Orbitfile
description: Orbitfile is the single TOML file for blueprint defaults, sync rules, and template-driven runs in Orbit.
---

`Orbitfile` is Orbit's blueprint control center.


Keep it in your blueprint root, and Orbit will discover the nearest `Orbitfile` automatically when you run from that directory tree.

## How Orbit uses Orbitfile

- `orbit run <path> --on <cluster>` or `orbit job run <path> --on <cluster>`: discovers the nearest ancestor directory containing `Orbitfile`.
- `orbit run <blueprint:tag> --on <cluster>` or `orbit blueprint run ... --on <cluster>`: uses registered blueprint metadata from Orbitfile.
- `orbit blueprint build`: uses Orbitfile metadata and sync rules while packaging.

This keeps runs reproducible: the same blueprint name, defaults, filters, and templating rules travel with the blueprint.

## Orbitfile sections

- `[blueprint]` (required): defines `name` (must match `^[A-Za-z0-9_-]+$`).
- `[retrieve]` (optional): `default_path` for `orbit job retrieve`.
- `[submit]` (optional): `sbatch_script` default for run commands.
- `[sync]` (optional): `include`/`exclude` rules for synced files.
- `[template]` (optional): enables templating features.

## Templates

Templates shine when your job structure stays the same, but run parameters change.

### Example: one template, many runs

Imagine the same pipeline is run for multiple samples. You want:

- one reusable sbatch file
- different run names and step counts
- cluster-specific partition/account at run time

An Orbitfile like this gives you that flow:

```toml
[blueprint]
name = "rna_analysis"

[submit]
sbatch_script = "sbatch/run.sbatch"

[template]

[template.files]
paths = ["sbatch/run.sbatch", "configs/run.yaml"]

[template.fields.sample_name]
type = "string"
default = "demo"
description = "Name shown in logs and outputs"

[template.fields.num_steps]
type = "integer"
default = 500

[template.fields.mode]
type = "enum"
values = ["fast", "accurate"]
default = "fast"

[template.presets.production]
mode = "accurate"
num_steps = 5000
```

And your sbatch template can use both normal fields and special Orbit variables:

```bash
#!/bin/bash
#SBATCH --job-name={{ sample_name }}
#SBATCH --partition={{ ORBIT_SLURM_PARTITION }}
#SBATCH --account={{ ORBIT_SLURM_ACCOUNT }}
export SCRATCH_DIR="{{ ORBIT_SCRATCH_DIRECTORY }}"
echo "mode={{ mode }} steps={{ num_steps }}"
```

Then each run becomes simple:

```bash
orbit run . --on cluster-a --field sample_name=pilot_01
```

```bash
orbit run . --on cluster-a --preset production --field sample_name=batch_2026_02
```

### Variable types

Orbit supports these template field types in `[template.fields.<name>].type`.

#### `string`

Best for free-form values like run names.

```toml
[template.fields.sample_name]
type = "string"
default = "demo"
```

Template usage: `{{ sample_name }}`

#### `enum`

Best when users must choose one valid option.

```toml
[template.fields.mode]
type = "enum"
values = ["fast", "accurate"]
default = "fast"
```

Template usage: `{{ mode }}`

#### `integer`

Best for whole-number parameters like counts or epochs.

```toml
[template.fields.num_steps]
type = "integer"
default = 500
```

Template usage: `{{ num_steps }}`

#### `float`

Best for decimal-valued parameters.

```toml
[template.fields.learning_rate]
type = "float"
default = 0.005
```

Template usage: `{{ learning_rate }}`

#### `json`

Best for structured config passed as object/array/scalar.

```toml
[template.fields.resources]
type = "json"
default = { gpus = 1, memory = "32G" }
```

Template usage: `{{ resources.gpus }}` and `{{ resources.memory }}`

#### `file_path`

Best when a template needs a validated local filesystem path. Relative paths are resolved from blueprint root.

```toml
[template.fields.input_file]
type = "file_path"
```

Template usage: `{{ input_file }}`

#### `file_contents`

Best when you want Orbit to read file text from a local file and inject it into rendered templates. Relative paths are resolved from blueprint root.

```toml
[template.fields.license_text]
type = "file_contents"
```

Template usage: `{{ license_text }}`

### How rendering works

1. List target files in `[template.files].paths`.
2. Orbit renders only those files using Tera syntax (`{{ ... }}`), UTF-8 text only, auto-escaping disabled.
3. Files not listed there are transferred unchanged.

`[template.files].paths` entries are blueprint-root-relative and must stay inside the run root.

### Value resolution order

Orbit resolves values in this order:

1. Field `default`
2. Preset (`--preset`)
3. Explicit overrides (`--field KEY=VALUE`)
4. Interactive input for missing values

In interactive mode, Orbit asks to confirm/edit fields unless you pass `--fill-defaults`.
In non-interactive mode, required values must already be provided.

### Special variables

Templates also support built-in special variables with `ORBIT_` names. These are used directly in template files and are not declared in `[template.fields]`.

Supported special variables:

- `ORBIT_SLURM_PARTITION`
- `ORBIT_SLURM_ACCOUNT`
- `ORBIT_SCRATCH_DIRECTORY`

Notes:

- Orbit auto-detects referenced special variables from your templated files.
- You can override them via `--field ORBIT_...=value`.
- Unknown `ORBIT_*` names fail validation early.
- In non-interactive mode, pass `--field ORBIT_...=...` if Orbit cannot resolve a special value automatically.

## Create an Orbitfile quickly

```bash
orbit blueprint init . --name rna_analysis
```
