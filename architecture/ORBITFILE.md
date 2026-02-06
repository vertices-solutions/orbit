# Orbitfile

The Orbitfile is the TOML configuration file that defines a project. Orbit discovers the
nearest ancestor directory containing an `Orbitfile` and uses it as the project root when
submitting or checking a project.

## Projects

A project is a directory tree rooted at the nearest directory containing `Orbitfile`.
Projects can be registered locally with `project init`, listed, checked, and submitted.
The registry keeps the project name and root path, while submit-time metadata (such as
`default_retrieve_path`) is copied onto each job so later retrieval is stable even if the
Orbitfile changes.

Key rules:
- Project root: nearest ancestor directory containing `Orbitfile`.
- Project name: value of `[project].name` in the Orbitfile.
- Name format: `^[A-Za-z0-9_-]+$` (letters, numbers, `_`, `-`).
- `project submit <name>` loads the registered project and uses its Orbitfile.
- `job submit <path>` auto-discovers Orbitfile from the submit path when present.

## Orbitfile Format

Orbitfile is TOML. The only required table is `[project]`. All other tables are optional.

### `[project]`
Required.

Fields:
- `name` (string): stable project identifier. Must match `^[A-Za-z0-9_-]+$`.

### `[retrieve]`
Optional.

Fields:
- `default_path` (string): default remote path for `job retrieve` when no path is provided.

### `[submit]`
Optional.

Fields:
- `sbatch_script` (string): relative or absolute path to the sbatch script.

Rules:
- Must resolve inside the submit root.
- Selection precedence: CLI `--sbatchscript` > `[submit].sbatch_script` > auto-detected `.sbatch`.

### `[sync]`
Optional.

Fields:
- `include` (array of strings): glob-style include patterns.
- `exclude` (array of strings): glob-style exclude patterns.

Rules:
- Patterns are matched against paths relative to the submit root using `/` separators.
- CLI include/exclude rules are applied before Orbitfile rules.

### `[template]`
Optional. When present, templating is enabled. If `paths` is empty, no files are rendered.

Notes:
- `[template]` can be empty. Template fields, files, and presets are defined in the tables below.

### `[template.fields.<name>]`
Optional. Defines a template field.

Fields:
- `type` (string, required): `string`, `integer`, `float`, `json`, `file_path`, `file_contents`, `enum`.
- `default` (optional): default value; must match the field type.
- `description` (optional): help text for prompts.
- `values` (array of strings, required for `enum`): allowed values.

Rules:
- Field names must match `^[A-Za-z_][A-Za-z0-9_]*$`.
- `file_path` values must exist on disk.
- `file_contents` values must exist on disk and are loaded as UTF-8 text.

### `[template.files]`
Optional.

Fields:
- `paths` (array of strings): list of template file paths relative to the project root.

Rules:
- Paths must be relative to the project root.
- Files are treated as UTF-8 text and rendered with Tera (auto-escaping disabled).
- All other files are transferred without rendering.

### `[template.presets.<preset_name>]`
Optional. Defines a set of default values for template fields.

Rules:
- Preset field names must match existing template field names.
- Presets are applied before explicit `--field key=value` overrides.

## Example

```toml
[project]
name = "protein-folding"

[retrieve]
default_path = "outputs/"

[submit]
sbatch_script = "sbatch/run.sbatch"

[sync]
include = ["src/**", "configs/**"]
exclude = ["**/*.tmp", "/.git/"]

[template]

[template.files]
paths = ["configs/run.yaml", "sbatch/run.sbatch"]

[template.fields.sample_name]
type = "string"
default = "demo"

[template.fields.num_steps]
type = "integer"

[template.fields.mode]
type = "enum"
values = ["fast", "accurate"]
default = "fast"

[template.fields.input_path]
type = "file_path"

[template.fields.license_text]
type = "file_contents"

[template.presets.fast]
num_steps = 500
mode = "fast"
```
