# Templates Proposal

## Summary
Add submit-time templating that allows users to fill project-specific fields and render them into files.
Fields are declared once in the Orbitfile, template files are listed separately, and values can be
provided via presets or user input at submit time. Rendering happens in `orbitd` using Tera so the
syntax stays Jinja-like. The daemon stages a temporary submit directory where non-templated files
are linked and templated files are rendered, then syncs the staged directory to the remote host.

## Goals
- Define a clear Orbitfile schema for template fields, templated files, and presets.
- Support field types: string, integer, float, json, file path, file contents, enum.
- Allow defaults and presets for any subset of fields.
- Implement templating in `orbitd` using Tera, with a staging directory before sync.
- Persist template values with the job as JSON in the database.

## Non-goals
- Complex template inheritance or partials across project files (basic file templating only).
- Binary templating (all templated files are treated as UTF-8 text).
- Template evaluation on the remote host.

## Orbitfile schema
Orbitfile uses TOML today, so templating follows that format. The new top-level table is `[template]`.

### Fields
Define fields under `[template.fields.<name>]`:
- `type` (required): `string`, `integer`, `float`, `json`, `file_path`, `file_contents`
- `default` (optional): type-dependent default value
- `description` (optional): used for CLI prompts/help
- `values` (enum only, required): list of allowed values

If a field has no default, a value is required at submit time.

### Files
Templated files are listed under `[[template.files]]`:
- `path` (required): file path relative to project root

Only files listed here are rendered. All other files are linked into the staging directory as-is.
Fields are global and can be referenced from any templated file.

### Presets
Presets live under `[template.presets.<preset_name>]` and provide values for any subset of fields.

### Example Orbitfile
```toml
[project]
name = "protein-folding"

[template]

[template.fields.sample_name]
type = "string"
default = "demo"

[template.fields.num_steps]
type = "integer"

[template.fields.temperature]
type = "float"
default = 298.15

[template.fields.config]
type = "json"

[template.fields.mode]
type = "enum"
values = ["fast", "accurate"]
default = "fast"

[template.fields.input_path]
type = "file_path"

[template.fields.license_text]
type = "file_contents"

[[template.files]]
path = "configs/run.yaml"

[[template.files]]
path = "sbatch/run.sbatch"

[template.presets.fast]
num_steps = 500
temperature = 310.0

[template.presets.prod]
num_steps = 2000
temperature = 295.0
```

### Example template file
```yaml
name: "{{ sample_name }}"
steps: {{ num_steps }}
temperature: {{ temperature }}
config: {{ config }}
input: "{{ input_path }}"
license: |
  {{ license_text }}
```

## Field semantics
- `string`: user input is stored and inserted verbatim.
- `integer`: parsed as i64; reject non-integers.
- `float`: parsed as f64; reject non-numbers.
- `json`: parsed into `serde_json::Value` so nested values are available in templates.
- `file_path`: user supplies a path; orbitd validates it exists and inserts the path string.
- `file_contents`: user supplies a path; orbitd validates it exists, reads UTF-8 contents, and inserts
  those contents into the template. The file path itself is not inserted unless a separate field is
  defined for it.
- `enum`: user supplies one of the allowed string values defined in `values`.

All values are available as top-level Tera variables named after the field (e.g., `{{ num_steps }}`).
Auto-escaping is disabled for template rendering because the content is not HTML.

## CLI UX (proposal)
This is optional but recommended for a smooth submit workflow:
- `orbit submit` reads Orbitfile templates and prompts for values in interactive mode.
- By default, all fields (including defaulted ones) are confirmed; `--fill-defaults` skips prompts
  for fields that already have defaults.
- Values can be provided explicitly with `--field key=value` (repeatable).
- `--preset <name>` applies a preset before prompting; explicit `--field` overrides the preset.

Non-interactive mode should require all missing values to be provided via flags (or defaults).

## RPC and data model changes (proposal)
To pass typed template values from the CLI to `orbitd`, extend `SubmitRequestInit`:
- `optional string template_values_json = N;` (JSON object keyed by field name)

The JSON should preserve types (numbers as numbers, JSON as objects/arrays). `orbitd` validates and
merges these with Orbitfile defaults and preset values.

Database changes:
- Add `template_values` (TEXT) to `jobs` for JSON-encoded values used in the submit.
- Extend `NewJob`/`JobRecord` to carry the JSON string (or parsed value) as needed.

Suggested JSON format:
```json
{
  "sample_name": "demo",
  "num_steps": 500,
  "temperature": 310.0,
  "config": { "mode": "fast" },
  "input_path": "/data/input.csv",
  "license_text": "..."
}
```

## `orbitd` submission flow (proposed)
1. Read Orbitfile at submit root and parse `[template]` if present.
2. Collect values: Orbitfile defaults + preset (if any) + user-provided values.
3. Validate types and required fields, and resolve file paths:
   - `file_path`: ensure path exists.
   - `file_contents`: ensure path exists and read UTF-8 contents.
4. Create a temporary staging directory.
5. Walk the submit root:
   - For files listed in `[[template.files]]`: read, render with Tera, and write to the staging path.
   - For all other files: create a hard link in staging (fallback to copy if hard-link fails).
6. Use the staging directory as the local sync root.
7. Submit the job as usual, but store `template_values` in the job row.
8. Clean up the staging directory after submit completes (success or failure).

## Validation and error handling
- Unknown fields in input or presets: error with a clear message.
- Missing required fields: error before any remote activity.
- Invalid number/JSON: error before templating.
- Missing template file or file path: error before templating.
- Template render errors (unknown variables, syntax errors): fail submit with detail.

## Compatibility
- Existing projects without `[template]` remain unchanged.
- CLI only prompts for template values when `[template]` is present.
- Orbitfile parsing in the CLI can ignore `[template]` unless the submit path needs it.

## Implementation plan (aligned with ARCHITECTURE.md)
1. **Orbitfile parsing (orbit)**: Extend `orbit/src/app/services/project.rs` with a
   `RawTemplate` section and parsed representation (fields, files, presets). Keep this in the
   project service module alongside existing Orbitfile parsing, as described in
   `docs/ARCHITECTURE.md` under the `project` service module responsibilities.
2. **CLI input flow (orbit)**: Update the submit handler to gather template values:
   - Interactive mode: prompt for all fields by default; `--fill-defaults` only prompts for fields
     without defaults.
   - Non-interactive: require all missing values or defaults.
   - Apply preset selection and `--field key=value` overrides in the CLI layer only.
   This keeps `orbit` responsible for interface and `orbitd` for functionality.
3. **Proto contract (proto)**: Extend `SubmitRequestInit` with `template_values_json` to pass
   a typed JSON map to `orbitd`. Regenerate code via `proto/build.rs`.
4. **Use-cases (orbitd)**: In `UseCases::submit`, load Orbitfile, validate template
   definitions, merge defaults + preset + submitted values, and reject unknown fields before any
   remote activity. This mirrors current submit validation behavior.
5. **Templating service (orbitd)**: Add a new helper module under
   `orbitd/src/app/services/` (e.g., `templates.rs`) that:
   - Resolves template file list.
   - Builds a Tera `Context` and renders files.
   - Stages a temporary directory, linking non-templated files and copying+rendering templated ones.
   This keeps the logic in a pure service as per the architecture guidelines.
6. **File sync adapter (orbitd)**: Update the submit flow to sync from the staging directory
   rather than the original submit root when templating is enabled.
7. **Persistence (orbitd)**: Add `template_values` to the `jobs` table, plus fields in
   `NewJob`/`JobRecord`, and thread through `JobStorePort` and the SQLite adapter. Store the JSON
   string from the final merged values.
8. **Tests**:
   - Unit tests for Orbitfile parsing and validation in `orbit`.
   - Unit tests for templating and staging in `orbitd` services.
   - DB migration and job persistence tests in `orbitd` store layer.

## Open questions
- Should file paths be constrained to the submit root? The current requirement only says to validate
  existence, but constraining paths could avoid accidental leakage of unrelated files.
- Do we need to expose template values in `list-jobs` responses, or keep them internal for now?
- Do we want to support per-file field allowlists later (e.g., `fields = ["a", "b"]`)?

## Testing plan (proposal)
- Unit tests for Orbitfile template parsing and validation.
- Unit tests for type conversions, file path validation, and file contents loading.
- Integration test to submit a project with templates and verify rendered files in the staging dir.
- DB migration test to ensure `template_values` is persisted and readable.
