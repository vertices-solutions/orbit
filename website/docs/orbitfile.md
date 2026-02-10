---
title: Orbitfile
description: Orbitfile is a small TOML config that defines project identity and defaults for build, submit, and retrieval.
---

`Orbitfile` is Orbit's project configuration file.

It is a small TOML file stored in your project root. Orbit reads it for :
- **Project definition**: the main project features are built around using Orbitfile
-  **`orbit job submit`**: if you want to submit a job that needs some templating, or you want to permanently override some defaults, but projects don't quite fit your needs - you can just add an Orbitfile in the local directory you're planning to submit to cluster and put the templating/configuration there.

In practice, Orbitfile helps you keep submissions reproducible by storing stable project metadata and default behavior in one place.

## What do you put in Orbitfile

- **Project name** (`[project].name`, can include ASCII characters, numbers, '-' and '_' symbols): used by `orbit project build` and `orbit project submit`.
- **Default retrieve path** (`[retrieve].default_path`): if your retrieve path is predictable, you can set a default relative file/directory path to be retrieved.
- **Default sbatch script** (`[submit].sbatch_script`): for `orbit project submit` and  `orbit job submit`. This might be helpful if most of time you're using a specific sbatch script and only rarely do need to override it.
- **Sync rules**: controls what is synced with include/exclude rules (`[sync]`) when Orbit prepares a submit. By default, this includes `.git` directory: you might want to also exclude some testing directory of yours. **IMPORTANT**: `project build` will also respect those rules by default to save your disk space. 
- **Templates and presets**: (`[template]`) section allows for parameterized submits (both for `orbit project submit` and `orbit job submit`). `[template.presets.<preset name>]` allows you to create template presets: group of values for template fields. 

## Minimal example

```toml
[project]
name = "rna_analysis"

[submit]
sbatch_script = "submit.sbatch"

[retrieve]
default_path = "results"

[sync]
exclude = ["/.git/", "*.tmp"]
include = ["config/*.yaml"]
```

You can generate an Orbitfile with:

```bash
orbit project init . --name rna_analysis
```


When you run `orbit job submit --to <cluster> <path>` from this project (or a subdirectory), Orbit automatically discovers the nearest Orbitfile and applies these defaults.
