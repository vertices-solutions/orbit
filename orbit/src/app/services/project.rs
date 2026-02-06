// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use proto::{SubmitPathFilterAction, SubmitPathFilterRule};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::app::errors::{AppError, AppResult};
use crate::app::ports::FilesystemPort;

const ORBITFILE_NAME: &str = "Orbitfile";

/// Holds include/exclude sync patterns resolved from Orbitfile.
/// This is used to merge project-level rules with CLI submit filters.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ProjectRuleSet {
    pub include: Vec<String>,
    pub exclude: Vec<String>,
}

/// Parsed and validated project configuration loaded from Orbitfile
/// This is the canonical in-memory representation used by project discovery and validation.
#[derive(Debug, Clone, PartialEq)]
pub struct OrbitfileProjectConfig {
    pub root: PathBuf,
    pub name: String,
    pub default_retrieve_path: Option<String>,
    pub submit_sbatch_script: Option<String>,
    pub rules: ProjectRuleSet,
    pub template: Option<TemplateConfig>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TemplateConfig {
    pub fields: BTreeMap<String, TemplateField>,
    pub files: Vec<String>,
    pub presets: BTreeMap<String, BTreeMap<String, JsonValue>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TemplateFieldType {
    String,
    Integer,
    Float,
    Json,
    FilePath,
    FileContents,
    Enum,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TemplateField {
    pub field_type: TemplateFieldType,
    pub default: Option<JsonValue>,
    pub description: Option<String>,
    pub enum_values: Vec<String>,
}

/// Result of checking a registered project on disk.
/// Captures success/failure and a human-readable reason for failures.
/// CheckedProjectStatus because ProjectCheckStatus is a type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckedProjectStatus {
    pub name: String,
    pub ok: bool,
    pub reason: Option<String>,
}

pub type ProjectCheckFailure = CheckedProjectStatus;
pub type ProjectCheckStatus = CheckedProjectStatus;

/// Top-level Orbitfile representation for serialization/deserialization.
/// Each section is optional so missing tables can be detected and defaulted.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct RawOrbitfile {
    #[serde(default)]
    project: Option<RawProject>,
    #[serde(default)]
    retrieve: Option<RawRetrieve>,
    #[serde(default)]
    submit: Option<RawSubmit>,
    #[serde(default)]
    sync: Option<RawSync>,
    #[serde(default)]
    template: Option<RawTemplate>,
}

/// Raw [project] section used while loading and writing Orbitfiles.
/// The name is validated after parsing.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct RawProject {
    name: String,
}

/// Raw [retrieve] section used while loading and writing Orbitfiles.
/// Carries the optional default retrieve path as-is before normalization.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct RawRetrieve {
    #[serde(default)]
    default_path: Option<String>,
}

/// Raw [submit] section used while loading and writing Orbitfiles.
/// Carries the optional sbatch script path as written in the file.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct RawSubmit {
    #[serde(default)]
    sbatch_script: Option<String>,
}

/// Raw [sync] section used while loading and writing Orbitfiles.
/// Includes are applied before excludes when merged into submit filters.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct RawSync {
    #[serde(default)]
    include: Vec<String>,
    #[serde(default)]
    exclude: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct RawTemplate {
    #[serde(default)]
    fields: BTreeMap<String, RawTemplateField>,
    #[serde(default)]
    files: RawTemplateFiles,
    #[serde(default)]
    presets: BTreeMap<String, BTreeMap<String, toml::Value>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct RawTemplateField {
    #[serde(rename = "type")]
    field_type: String,
    #[serde(default)]
    default: Option<toml::Value>,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    values: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct RawTemplateFiles {
    #[serde(default)]
    paths: Vec<String>,
}

pub fn validate_project_name(name: &str) -> AppResult<()> {
    let trimmed = name.trim();
    if trimmed.is_empty() {
        return Err(AppError::invalid_argument("project name cannot be empty"));
    }
    let valid = trimmed
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '_');
    if !valid {
        return Err(AppError::invalid_argument(
            "project name must match ^[A-Za-z0-9_-]+$",
        ));
    }
    Ok(())
}

pub fn sanitize_project_name(input: &str) -> String {
    input
        .trim()
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric() || *ch == '-' || *ch == '_')
        .collect()
}

pub fn build_default_orbitfile_contents(name: &str) -> AppResult<String> {
    validate_project_name(name)?;
    let mut raw = RawOrbitfile {
        project: Some(RawProject {
            name: name.trim().to_string(),
        }),
        ..RawOrbitfile::default()
    };
    raw.sync = Some(RawSync {
        include: Vec::new(),
        exclude: vec!["/.git/".to_string()],
    });
    raw.template = Some(RawTemplate::default());
    toml::to_string(&raw).map_err(|err| AppError::local_error(err.to_string()))
}

pub fn upsert_orbitfile_project_name(existing: Option<&str>, name: &str) -> AppResult<String> {
    validate_project_name(name)?;
    let mut raw = match existing {
        Some(contents) => toml::from_str::<RawOrbitfile>(contents)
            .map_err(|err| AppError::invalid_argument(format!("invalid Orbitfile: {err}")))?,
        None => RawOrbitfile::default(),
    };
    raw.project = Some(RawProject {
        name: name.trim().to_string(),
    });
    if raw.sync.is_none() {
        raw.sync = Some(RawSync {
            include: Vec::new(),
            exclude: vec!["/.git/".to_string()],
        });
    }
    toml::to_string(&raw).map_err(|err| AppError::local_error(err.to_string()))
}

pub fn discover_project_from_submit_root(
    fs: &dyn FilesystemPort,
    submit_root: &Path,
) -> AppResult<Option<OrbitfileProjectConfig>> {
    let mut cursor = if fs.is_dir(submit_root)? {
        submit_root.to_path_buf()
    } else {
        submit_root
            .parent()
            .map(Path::to_path_buf)
            .ok_or_else(|| AppError::invalid_argument("submit path has no parent directory"))?
    };

    loop {
        let orbitfile = cursor.join(ORBITFILE_NAME);
        if fs.is_file(&orbitfile)? {
            let config = load_project_from_root(fs, &cursor)?;
            return Ok(Some(config));
        }
        if !cursor.pop() {
            break;
        }
    }
    Ok(None)
}

pub fn load_project_from_root(
    fs: &dyn FilesystemPort,
    project_root: &Path,
) -> AppResult<OrbitfileProjectConfig> {
    let orbitfile_path = project_root.join(ORBITFILE_NAME);
    if !fs.is_file(&orbitfile_path)? {
        return Err(AppError::invalid_argument(format!(
            "Project has no Orbitfile at {}",
            orbitfile_path.display()
        )));
    }

    let bytes = fs.read_file(&orbitfile_path)?;
    let content = String::from_utf8(bytes)
        .map_err(|err| AppError::invalid_argument(format!("invalid Orbitfile encoding: {err}")))?;
    let raw = toml::from_str::<RawOrbitfile>(&content)
        .map_err(|err| AppError::invalid_argument(format!("invalid Orbitfile: {err}")))?;

    let RawOrbitfile {
        project,
        retrieve,
        submit,
        sync,
        template,
    } = raw;

    let project =
        project.ok_or_else(|| AppError::invalid_argument("Orbitfile is missing [project]"))?;
    let name = project.name.trim().to_string();
    validate_project_name(&name)?;

    let default_retrieve_path = trim_optional(retrieve.and_then(|section| section.default_path))
        .map(|value| value.to_string());
    let submit_sbatch_script = trim_optional(submit.and_then(|section| section.sbatch_script))
        .map(|value| value.to_string());

    let mut include = Vec::new();
    let mut exclude = Vec::new();
    if let Some(sync) = sync {
        include = normalize_patterns(sync.include, "sync.include")?;
        exclude = normalize_patterns(sync.exclude, "sync.exclude")?;
    }

    let template = parse_template_config(template)?;

    Ok(OrbitfileProjectConfig {
        root: project_root.to_path_buf(),
        name,
        default_retrieve_path,
        submit_sbatch_script,
        rules: ProjectRuleSet { include, exclude },
        template,
    })
}

pub fn template_config_from_json(raw: &str) -> AppResult<TemplateConfig> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(AppError::invalid_argument(
            "template config JSON cannot be empty",
        ));
    }
    serde_json::from_str::<TemplateConfig>(trimmed).map_err(|err| {
        AppError::invalid_argument(format!("invalid template config JSON: {err}"))
    })
}

pub fn merge_submit_filters(
    cli_filters: Vec<SubmitPathFilterRule>,
    rules: &ProjectRuleSet,
) -> Vec<SubmitPathFilterRule> {
    let mut merged = cli_filters;
    for pattern in &rules.include {
        merged.push(SubmitPathFilterRule {
            action: SubmitPathFilterAction::Include as i32,
            pattern: pattern.clone(),
        });
    }
    for pattern in &rules.exclude {
        merged.push(SubmitPathFilterRule {
            action: SubmitPathFilterAction::Exclude as i32,
            pattern: pattern.clone(),
        });
    }
    merged
}

pub fn resolve_orbitfile_sbatch_script(
    fs: &dyn FilesystemPort,
    project_root: &Path,
    submit_root: &Path,
    sbatch_script: &str,
) -> AppResult<String> {
    let trimmed = sbatch_script.trim();
    if trimmed.is_empty() {
        return Err(AppError::invalid_argument(
            "[submit].sbatch_script cannot be empty",
        ));
    }
    let submit_root_canonical = fs.canonicalize(submit_root)?;
    let candidate = PathBuf::from(trimmed);
    let candidate = if candidate.is_absolute() {
        candidate
    } else {
        project_root.join(candidate)
    };
    let script_canonical = fs.canonicalize(&candidate).map_err(|err| {
        AppError::invalid_argument(format!(
            "invalid [submit].sbatch_script '{}': {}",
            trimmed, err.message
        ))
    })?;
    if !script_canonical.starts_with(&submit_root_canonical) {
        return Err(AppError::invalid_argument(format!(
            "invalid [submit].sbatch_script '{}': path must resolve inside submit root '{}'",
            trimmed,
            submit_root.display()
        )));
    }
    if !fs.is_file(&script_canonical)? {
        return Err(AppError::invalid_argument(format!(
            "invalid [submit].sbatch_script '{}': target is not a file",
            trimmed
        )));
    }
    let rel = script_canonical
        .strip_prefix(&submit_root_canonical)
        .map_err(|_| AppError::invalid_argument("failed to relativize sbatch script path"))?;
    if rel.as_os_str().is_empty() {
        return Err(AppError::invalid_argument(
            "invalid [submit].sbatch_script: relative path is empty",
        ));
    }
    Ok(rel.to_string_lossy().replace('\\', "/"))
}

pub fn check_registered_project(
    fs: &dyn FilesystemPort,
    name: &str,
    path: &Path,
) -> ProjectCheckStatus {
    let base_name = name.split_once(':').map(|(base, _)| base).unwrap_or(name);
    if !fs.is_dir(path).unwrap_or(false) {
        return CheckedProjectStatus {
            name: name.to_string(),
            ok: false,
            reason: Some(format!(
                "Project {} path does not exist: {}",
                name,
                path.display()
            )),
        };
    }
    let orbitfile_path = path.join(ORBITFILE_NAME);
    if !fs.is_file(&orbitfile_path).unwrap_or(false) {
        return CheckedProjectStatus {
            name: name.to_string(),
            ok: false,
            reason: Some(format!(
                "Project {} has no Orbitfile at {}",
                name,
                orbitfile_path.display()
            )),
        };
    }

    let config = match load_project_from_root(fs, path) {
        Ok(config) => config,
        Err(err) => {
            return CheckedProjectStatus {
                name: name.to_string(),
                ok: false,
                reason: Some(format!(
                    "Project {} has invalid Orbitfile: {}",
                    name, err.message
                )),
            };
        }
    };

    if config.name != base_name {
        return CheckedProjectStatus {
            name: name.to_string(),
            ok: false,
            reason: Some(format!(
                "Project {} has invalid Orbitfile: [project].name '{}' does not match registry name '{}'",
                name, config.name, base_name
            )),
        };
    }

    if let Some(ref sbatch_script) = config.submit_sbatch_script
        && let Err(err) = resolve_orbitfile_sbatch_script(fs, path, path, sbatch_script)
    {
        return CheckedProjectStatus {
            name: name.to_string(),
            ok: false,
            reason: Some(format!(
                "Project {} has invalid [submit].sbatch_script: {}",
                name, err.message
            )),
        };
    }

    CheckedProjectStatus {
        name: name.to_string(),
        ok: true,
        reason: None,
    }
}

fn trim_optional(value: Option<String>) -> Option<String> {
    value.and_then(|item| {
        let trimmed = item.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
}

fn normalize_patterns(values: Vec<String>, field: &str) -> AppResult<Vec<String>> {
    let mut out = Vec::with_capacity(values.len());
    for value in values {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            return Err(AppError::invalid_argument(format!(
                "Orbitfile {field} contains an empty pattern"
            )));
        }
        out.push(trimmed.to_string());
    }
    Ok(out)
}

fn parse_template_config(raw: Option<RawTemplate>) -> AppResult<Option<TemplateConfig>> {
    let Some(raw) = raw else {
        return Ok(None);
    };

    let mut fields = BTreeMap::new();
    for (name, raw_field) in raw.fields {
        let field_name = name.trim();
        if field_name.is_empty() {
            return Err(AppError::invalid_argument(
                "template field name cannot be empty",
            ));
        }
        validate_template_field_name(field_name)?;
        let field_type = parse_template_field_type(&raw_field.field_type)?;
        let enum_values = normalize_enum_values(field_name, field_type, raw_field.values)?;
        let field = TemplateField {
            field_type,
            default: None,
            description: trim_optional(raw_field.description),
            enum_values,
        };
        let default = match raw_field.default {
            Some(value) => Some(
                parse_template_value_from_toml(&field, &value).map_err(|err| {
                    AppError::invalid_argument(format!(
                        "template field '{field_name}' default is invalid: {}",
                        err.message
                    ))
                })?,
            ),
            None => None,
        };
        let field = TemplateField { default, ..field };
        fields.insert(field_name.to_string(), field);
    }

    let mut files = Vec::new();
    let mut seen_files = BTreeSet::new();
    for file in raw.files.paths {
        let trimmed = file.trim();
        if trimmed.is_empty() {
            return Err(AppError::invalid_argument(
                "template file path cannot be empty",
            ));
        }
        if !seen_files.insert(trimmed.to_string()) {
            return Err(AppError::invalid_argument(format!(
                "template file '{trimmed}' is listed more than once"
            )));
        }
        files.push(trimmed.to_string());
    }

    let mut presets = BTreeMap::new();
    for (preset_name, values) in raw.presets {
        let preset_key = preset_name.trim();
        if preset_key.is_empty() {
            return Err(AppError::invalid_argument(
                "template preset name cannot be empty",
            ));
        }
        if presets.contains_key(preset_key) {
            return Err(AppError::invalid_argument(format!(
                "template preset '{preset_key}' is listed more than once"
            )));
        }
        let mut preset_values = BTreeMap::new();
        for (field_name, value) in values {
            let field_key = field_name.trim();
            if field_key.is_empty() {
                return Err(AppError::invalid_argument(format!(
                    "template preset '{preset_key}' contains an empty field name"
                )));
            }
            let Some(field) = fields.get(field_key) else {
                return Err(AppError::invalid_argument(format!(
                    "template preset '{preset_key}' references unknown field '{field_key}'"
                )));
            };
            let parsed = parse_template_value_from_toml(field, &value).map_err(|err| {
                AppError::invalid_argument(format!(
                    "template preset '{preset_key}' field '{field_key}' is invalid: {}",
                    err.message
                ))
            })?;
            preset_values.insert(field_key.to_string(), parsed);
        }
        presets.insert(preset_key.to_string(), preset_values);
    }

    Ok(Some(TemplateConfig {
        fields,
        files,
        presets,
    }))
}

fn validate_template_field_name(name: &str) -> AppResult<()> {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return Err(AppError::invalid_argument(
            "template field name cannot be empty",
        ));
    };
    if !(first.is_ascii_alphabetic() || first == '_') {
        return Err(AppError::invalid_argument(format!(
            "template field name '{name}' must start with a letter or underscore"
        )));
    }
    if !chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_') {
        return Err(AppError::invalid_argument(format!(
            "template field name '{name}' must match ^[A-Za-z_][A-Za-z0-9_]*$"
        )));
    }
    Ok(())
}

fn parse_template_field_type(value: &str) -> AppResult<TemplateFieldType> {
    match value.trim() {
        "string" => Ok(TemplateFieldType::String),
        "integer" => Ok(TemplateFieldType::Integer),
        "float" => Ok(TemplateFieldType::Float),
        "json" => Ok(TemplateFieldType::Json),
        "file_path" => Ok(TemplateFieldType::FilePath),
        "file_contents" => Ok(TemplateFieldType::FileContents),
        "enum" => Ok(TemplateFieldType::Enum),
        other => Err(AppError::invalid_argument(format!(
            "unknown template field type '{other}'"
        ))),
    }
}

fn parse_template_value_from_toml(
    field: &TemplateField,
    value: &toml::Value,
) -> AppResult<JsonValue> {
    match field.field_type {
        TemplateFieldType::String
        | TemplateFieldType::FilePath
        | TemplateFieldType::FileContents => match value {
            toml::Value::String(v) => Ok(JsonValue::String(v.clone())),
            _ => Err(AppError::invalid_argument(format!(
                "expected string, got {}",
                toml_type_name(value)
            ))),
        },
        TemplateFieldType::Integer => match value {
            toml::Value::Integer(v) => Ok(JsonValue::Number((*v).into())),
            toml::Value::Float(v) if v.fract() == 0.0 => Ok(JsonValue::Number((*v as i64).into())),
            _ => Err(AppError::invalid_argument(format!(
                "expected integer, got {}",
                toml_type_name(value)
            ))),
        },
        TemplateFieldType::Float => match value {
            toml::Value::Integer(v) => Ok(JsonValue::Number((*v).into())),
            toml::Value::Float(v) => serde_json::Number::from_f64(*v)
                .map(JsonValue::Number)
                .ok_or_else(|| {
                    AppError::invalid_argument("float value is not representable as JSON")
                }),
            _ => Err(AppError::invalid_argument(format!(
                "expected float, got {}",
                toml_type_name(value)
            ))),
        },
        TemplateFieldType::Json => toml_value_to_json(value),
        TemplateFieldType::Enum => match value {
            toml::Value::String(v) => {
                if field.enum_values.iter().any(|item| item == v) {
                    Ok(JsonValue::String(v.clone()))
                } else {
                    Err(AppError::invalid_argument(format!(
                        "expected one of [{}]",
                        field.enum_values.join(", ")
                    )))
                }
            }
            _ => Err(AppError::invalid_argument(format!(
                "expected string, got {}",
                toml_type_name(value)
            ))),
        },
    }
}

fn normalize_enum_values(
    field_name: &str,
    field_type: TemplateFieldType,
    values: Vec<String>,
) -> AppResult<Vec<String>> {
    if field_type != TemplateFieldType::Enum {
        if values.is_empty() {
            return Ok(Vec::new());
        }
        return Err(AppError::invalid_argument(format!(
            "template field '{field_name}' has enum values but is not type 'enum'"
        )));
    }
    let mut out = Vec::new();
    let mut seen = BTreeSet::new();
    for value in values {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            return Err(AppError::invalid_argument(format!(
                "template field '{field_name}' has an empty enum value"
            )));
        }
        if !seen.insert(trimmed.to_string()) {
            return Err(AppError::invalid_argument(format!(
                "template field '{field_name}' has duplicate enum value '{trimmed}'"
            )));
        }
        out.push(trimmed.to_string());
    }
    if out.is_empty() {
        return Err(AppError::invalid_argument(format!(
            "template field '{field_name}' enum values cannot be empty"
        )));
    }
    Ok(out)
}

fn toml_value_to_json(value: &toml::Value) -> AppResult<JsonValue> {
    match value {
        toml::Value::String(v) => Ok(JsonValue::String(v.clone())),
        toml::Value::Integer(v) => Ok(JsonValue::Number((*v).into())),
        toml::Value::Float(v) => serde_json::Number::from_f64(*v)
            .map(JsonValue::Number)
            .ok_or_else(|| AppError::invalid_argument("float value is not representable as JSON")),
        toml::Value::Boolean(v) => Ok(JsonValue::Bool(*v)),
        toml::Value::Datetime(v) => Ok(JsonValue::String(v.to_string())),
        toml::Value::Array(values) => {
            let mut out = Vec::with_capacity(values.len());
            for item in values {
                out.push(toml_value_to_json(item)?);
            }
            Ok(JsonValue::Array(out))
        }
        toml::Value::Table(table) => {
            let mut map = serde_json::Map::new();
            for (key, value) in table {
                map.insert(key.clone(), toml_value_to_json(value)?);
            }
            Ok(JsonValue::Object(map))
        }
    }
}

fn toml_type_name(value: &toml::Value) -> &'static str {
    match value {
        toml::Value::String(_) => "string",
        toml::Value::Integer(_) => "integer",
        toml::Value::Float(_) => "float",
        toml::Value::Boolean(_) => "boolean",
        toml::Value::Datetime(_) => "datetime",
        toml::Value::Array(_) => "array",
        toml::Value::Table(_) => "table",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn validate_project_name_accepts_expected_pattern() {
        assert!(validate_project_name("abc-123_name").is_ok());
        assert!(validate_project_name("bad space").is_err());
        assert!(validate_project_name("bad.dot").is_err());
    }

    #[test]
    fn merge_submit_filters_places_cli_first() {
        let cli = vec![SubmitPathFilterRule {
            action: SubmitPathFilterAction::Include as i32,
            pattern: "cli/**".to_string(),
        }];
        let merged = merge_submit_filters(
            cli,
            &ProjectRuleSet {
                include: vec!["src/**".to_string()],
                exclude: vec!["**/*.tmp".to_string()],
            },
        );
        assert_eq!(merged.len(), 3);
        assert_eq!(merged[0].pattern, "cli/**");
        assert_eq!(merged[1].pattern, "src/**");
        assert_eq!(merged[2].pattern, "**/*.tmp");
    }

    #[test]
    fn default_orbitfile_contains_project_and_git_exclude() {
        let content = build_default_orbitfile_contents("demo").expect("content");
        assert!(content.contains("[project]"));
        assert!(content.contains("name = \"demo\""));
        assert!(content.contains("[sync]"));
        assert!(content.contains("/.git/"));
        assert!(content.contains("[template.files]"));
        assert!(content.contains("paths = []"));
    }

    #[test]
    fn parse_template_config_accepts_fields_files_and_presets() {
        let raw = toml::from_str::<RawOrbitfile>(
            r#"
            [project]
            name = "demo"

            [template]

            [template.fields.sample_name]
            type = "string"
            default = "demo"

            [template.fields.num_steps]
            type = "integer"

            [template.fields.config]
            type = "json"
            default = { mode = "fast" }

            [template.fields.mode]
            type = "enum"
            values = ["fast", "accurate"]
            default = "fast"

            [template.files]
            paths = ["configs/run.yaml"]

            [template.presets.fast]
            num_steps = 500
            "#,
        )
        .expect("orbitfile");
        let template = parse_template_config(raw.template)
            .expect("parse")
            .expect("template");
        assert!(template.fields.contains_key("sample_name"));
        assert!(template.fields.contains_key("num_steps"));
        assert!(template.fields.contains_key("config"));
        let mode = template.fields.get("mode").expect("mode");
        assert_eq!(mode.enum_values, vec!["fast", "accurate"]);
        assert_eq!(template.files, vec!["configs/run.yaml"]);
        assert!(template.presets.contains_key("fast"));
    }

    #[test]
    fn toml_value_to_json_converts_supported_types() {
        let value = toml::Value::String("hello".to_string());
        assert_eq!(toml_value_to_json(&value).unwrap(), json!("hello"));

        let value = toml::Value::Integer(42);
        assert_eq!(toml_value_to_json(&value).unwrap(), json!(42));

        let value = toml::Value::Float(1.5);
        assert_eq!(toml_value_to_json(&value).unwrap(), json!(1.5));

        let value = toml::Value::Boolean(true);
        assert_eq!(toml_value_to_json(&value).unwrap(), json!(true));

        let value = toml::from_str::<toml::Value>("dt = 2024-01-02T03:04:05Z").unwrap();
        let dt = value.get("dt").unwrap();
        assert_eq!(
            toml_value_to_json(dt).unwrap(),
            json!("2024-01-02T03:04:05Z")
        );

        let value = toml::Value::Array(vec![
            toml::Value::Integer(1),
            toml::Value::String("a".to_string()),
        ]);
        assert_eq!(toml_value_to_json(&value).unwrap(), json!([1, "a"]));

        let mut table = toml::value::Table::new();
        table.insert("key".to_string(), toml::Value::Boolean(false));
        table.insert("nested".to_string(), toml::Value::Integer(9));
        let value = toml::Value::Table(table);
        assert_eq!(toml_value_to_json(&value).unwrap(), json!({"key": false, "nested": 9}));
    }

    #[test]
    fn normalize_enum_values_validates_types_and_values() {
        let values = normalize_enum_values("mode", TemplateFieldType::String, Vec::new()).unwrap();
        assert!(values.is_empty());

        let err =
            normalize_enum_values("mode", TemplateFieldType::String, vec!["fast".into()]).unwrap_err();
        assert!(err.message.contains("has enum values but is not type 'enum'"));

        let err =
            normalize_enum_values("mode", TemplateFieldType::Enum, vec!["".into()]).unwrap_err();
        assert!(err.message.contains("has an empty enum value"));

        let err = normalize_enum_values(
            "mode",
            TemplateFieldType::Enum,
            vec!["fast".into(), "fast".into()],
        )
        .unwrap_err();
        assert!(err.message.contains("has duplicate enum value"));

        let values = normalize_enum_values(
            "mode",
            TemplateFieldType::Enum,
            vec![" fast ".into(), "slow".into()],
        )
        .unwrap();
        assert_eq!(values, vec!["fast", "slow"]);
    }

    #[test]
    fn parse_template_field_type_accepts_known_values() {
        assert_eq!(
            parse_template_field_type(" string ").unwrap(),
            TemplateFieldType::String
        );
        assert_eq!(
            parse_template_field_type("integer").unwrap(),
            TemplateFieldType::Integer
        );
        assert!(parse_template_field_type("unknown").is_err());
    }

    #[test]
    fn validate_template_field_name_rejects_invalid_identifiers() {
        assert!(validate_template_field_name("name").is_ok());
        assert!(validate_template_field_name("_name").is_ok());
        assert!(validate_template_field_name("9name").is_err());
        assert!(validate_template_field_name("bad-name").is_err());
    }

    #[test]
    fn parse_template_value_from_toml_handles_field_types() {
        let string_field = TemplateField {
            field_type: TemplateFieldType::String,
            default: None,
            description: None,
            enum_values: Vec::new(),
        };
        let value = toml::Value::String("alpha".to_string());
        assert_eq!(
            parse_template_value_from_toml(&string_field, &value).unwrap(),
            json!("alpha")
        );

        let int_field = TemplateField {
            field_type: TemplateFieldType::Integer,
            default: None,
            description: None,
            enum_values: Vec::new(),
        };
        let value = toml::Value::Float(10.0);
        assert_eq!(
            parse_template_value_from_toml(&int_field, &value).unwrap(),
            json!(10)
        );

        let float_field = TemplateField {
            field_type: TemplateFieldType::Float,
            default: None,
            description: None,
            enum_values: Vec::new(),
        };
        let value = toml::Value::Integer(7);
        assert_eq!(
            parse_template_value_from_toml(&float_field, &value).unwrap(),
            json!(7)
        );

        let enum_field = TemplateField {
            field_type: TemplateFieldType::Enum,
            default: None,
            description: None,
            enum_values: vec!["fast".to_string(), "slow".to_string()],
        };
        let value = toml::Value::String("fast".to_string());
        assert_eq!(
            parse_template_value_from_toml(&enum_field, &value).unwrap(),
            json!("fast")
        );
        let value = toml::Value::String("invalid".to_string());
        assert!(parse_template_value_from_toml(&enum_field, &value).is_err());

        let json_field = TemplateField {
            field_type: TemplateFieldType::Json,
            default: None,
            description: None,
            enum_values: Vec::new(),
        };
        let mut table = toml::value::Table::new();
        table.insert("mode".to_string(), toml::Value::String("fast".to_string()));
        let value = toml::Value::Table(table);
        assert_eq!(
            parse_template_value_from_toml(&json_field, &value).unwrap(),
            json!({"mode": "fast"})
        );
    }

    #[test]
    fn parse_template_config_rejects_invalid_inputs() {
        let raw = toml::from_str::<RawOrbitfile>(
            r#"
            [project]
            name = "demo"

            [template]

            [template.fields.count]
            type = "integer"
            default = "oops"
            "#,
        )
        .expect("orbitfile");
        assert!(parse_template_config(raw.template).is_err());

        let raw = toml::from_str::<RawOrbitfile>(
            r#"
            [project]
            name = "demo"

            [template]

            [template.fields."1bad"]
            type = "string"
            "#,
        )
        .expect("orbitfile");
        assert!(parse_template_config(raw.template).is_err());

        let raw = toml::from_str::<RawOrbitfile>(
            r#"
            [project]
            name = "demo"

            [template]

            [template.fields.mode]
            type = "string"

            [template.files]
            paths = ["a.txt", "a.txt"]
            "#,
        )
        .expect("orbitfile");
        assert!(parse_template_config(raw.template).is_err());

        let raw = toml::from_str::<RawOrbitfile>(
            r#"
            [project]
            name = "demo"

            [template]

            [template.fields.mode]
            type = "string"

            [template.presets.fast]
            unknown = "value"
            "#,
        )
        .expect("orbitfile");
        assert!(parse_template_config(raw.template).is_err());
    }
}
