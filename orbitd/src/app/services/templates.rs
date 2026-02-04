// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use tempfile::TempDir;
use tera::Context;
use walkdir::WalkDir;

use crate::app::errors::{AppError, AppErrorKind, AppResult, codes};

const ORBITFILE_NAME: &str = "Orbitfile";

#[derive(Debug)]
pub struct PreparedTemplate {
    pub temp_dir: TempDir,
    pub staging_root: PathBuf,
    pub values_json: String,
}

#[derive(Debug, Clone, PartialEq)]
struct TemplateConfig {
    fields: BTreeMap<String, TemplateField>,
    files: Vec<String>,
    presets: BTreeMap<String, BTreeMap<String, JsonValue>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TemplateFieldType {
    String,
    Integer,
    Float,
    Json,
    FilePath,
    FileContents,
    Enum,
}

#[derive(Debug, Clone, PartialEq)]
struct TemplateField {
    field_type: TemplateFieldType,
    default: Option<JsonValue>,
    description: Option<String>,
    enum_values: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct RawOrbitfile {
    #[serde(default)]
    template: Option<RawTemplate>,
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

#[derive(Debug)]
struct ResolvedTemplateValues {
    values: BTreeMap<String, JsonValue>,
    values_json: String,
}

#[derive(Debug)]
struct StagedProject {
    temp_dir: TempDir,
    root: PathBuf,
}

pub fn prepare_template_submission(
    submit_root: &Path,
    template_values_json: Option<&str>,
) -> AppResult<Option<PreparedTemplate>> {
    if !submit_root.is_dir() {
        return Err(invalid_argument(format!(
            "submit path '{}' is not a directory",
            submit_root.display()
        )));
    }

    let Some(project_root) = discover_orbitfile_root(submit_root)? else {
        if template_values_json.is_some() {
            return Err(invalid_argument(
                "template values provided but no Orbitfile was found",
            ));
        }
        return Ok(None);
    };

    let orbitfile_path = project_root.join(ORBITFILE_NAME);
    let template_config = load_template_config(&orbitfile_path)?;
    let Some(template_config) = template_config else {
        if template_values_json.is_some() {
            return Err(invalid_argument(
                "template values provided but Orbitfile has no [template] section",
            ));
        }
        return Ok(None);
    };

    let resolved_values =
        resolve_template_values(&template_config, template_values_json, &project_root)?;
    let staged = stage_project(
        submit_root,
        &project_root,
        &template_config,
        &resolved_values.values,
    )?;

    Ok(Some(PreparedTemplate {
        temp_dir: staged.temp_dir,
        staging_root: staged.root,
        values_json: resolved_values.values_json,
    }))
}

fn discover_orbitfile_root(submit_root: &Path) -> AppResult<Option<PathBuf>> {
    let mut cursor = submit_root.to_path_buf();
    loop {
        let orbitfile = cursor.join(ORBITFILE_NAME);
        if orbitfile.is_file() {
            return Ok(Some(cursor));
        }
        if !cursor.pop() {
            break;
        }
    }
    Ok(None)
}

fn load_template_config(orbitfile_path: &Path) -> AppResult<Option<TemplateConfig>> {
    let content = std::fs::read_to_string(orbitfile_path).map_err(|err| {
        local_error(format!(
            "failed to read Orbitfile {}: {err}",
            orbitfile_path.display()
        ))
    })?;
    let raw = toml::from_str::<RawOrbitfile>(&content).map_err(|err| {
        invalid_argument(format!(
            "invalid Orbitfile {}: {err}",
            orbitfile_path.display()
        ))
    })?;
    parse_template_config(raw.template)
}

fn parse_template_config(raw: Option<RawTemplate>) -> AppResult<Option<TemplateConfig>> {
    let Some(raw) = raw else {
        return Ok(None);
    };

    let mut fields = BTreeMap::new();
    for (name, raw_field) in raw.fields {
        let field_name = name.trim();
        if field_name.is_empty() {
            return Err(invalid_argument("template field name cannot be empty"));
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
                    invalid_argument(format!(
                        "template field '{field_name}' default is invalid: {}",
                        err.message()
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
            return Err(invalid_argument("template file path cannot be empty"));
        }
        if !seen_files.insert(trimmed.to_string()) {
            return Err(invalid_argument(format!(
                "template file '{trimmed}' is listed more than once"
            )));
        }
        files.push(trimmed.to_string());
    }

    let mut presets = BTreeMap::new();
    for (preset_name, values) in raw.presets {
        let preset_key = preset_name.trim();
        if preset_key.is_empty() {
            return Err(invalid_argument("template preset name cannot be empty"));
        }
        if presets.contains_key(preset_key) {
            return Err(invalid_argument(format!(
                "template preset '{preset_key}' is listed more than once"
            )));
        }
        let mut preset_values = BTreeMap::new();
        for (field_name, value) in values {
            let field_key = field_name.trim();
            if field_key.is_empty() {
                return Err(invalid_argument(format!(
                    "template preset '{preset_key}' contains an empty field name"
                )));
            }
            let Some(field) = fields.get(field_key) else {
                return Err(invalid_argument(format!(
                    "template preset '{preset_key}' references unknown field '{field_key}'"
                )));
            };
            let parsed = parse_template_value_from_toml(field, &value).map_err(|err| {
                invalid_argument(format!(
                    "template preset '{preset_key}' field '{field_key}' is invalid: {}",
                    err.message()
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

fn resolve_template_values(
    config: &TemplateConfig,
    template_values_json: Option<&str>,
    project_root: &Path,
) -> AppResult<ResolvedTemplateValues> {
    let mut values: BTreeMap<String, JsonValue> = BTreeMap::new();
    for (name, field) in &config.fields {
        if let Some(default) = field.default.as_ref() {
            values.insert(name.clone(), default.clone());
        }
    }

    if let Some(raw) = template_values_json {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Err(invalid_argument("template values JSON cannot be empty"));
        }
        let parsed: JsonValue = serde_json::from_str(trimmed)
            .map_err(|err| invalid_argument(format!("invalid template values JSON: {err}")))?;
        let Some(map) = parsed.as_object() else {
            return Err(invalid_argument("template values JSON must be an object"));
        };
        for (name, value) in map {
            let Some(field) = config.fields.get(name) else {
                return Err(invalid_argument(format!(
                    "template field '{name}' was not found",
                )));
            };
            validate_json_value(field, value).map_err(|err| {
                invalid_argument(format!("template field '{name}' is invalid: {err}"))
            })?;
            values.insert(name.clone(), value.clone());
        }
    }

    let mut missing = Vec::new();
    for (name, _field) in &config.fields {
        if !values.contains_key(name) {
            missing.push(name.clone());
        }
    }
    if !missing.is_empty() {
        return Err(invalid_argument(format!(
            "missing required template fields: {}",
            missing.join(", ")
        )));
    }

    for (name, field) in &config.fields {
        if !is_path_field(field.field_type) {
            continue;
        }
        let path_value = values
            .get(name)
            .and_then(|value| value.as_str())
            .map(|value| value.to_string())
            .ok_or_else(|| {
                invalid_argument(format!("template field '{name}' must be a string path"))
            })?;
        let resolved_path = resolve_path(&path_value, project_root);
        match field.field_type {
            TemplateFieldType::FilePath => {
                if !resolved_path.exists() {
                    return Err(invalid_argument(format!(
                        "template field '{name}' path does not exist: {}",
                        resolved_path.display()
                    )));
                }
            }
            TemplateFieldType::FileContents => {
                if !resolved_path.is_file() {
                    return Err(invalid_argument(format!(
                        "template field '{name}' path is not a file: {}",
                        resolved_path.display()
                    )));
                }
                let contents = std::fs::read_to_string(&resolved_path).map_err(|err| {
                    invalid_argument(format!(
                        "template field '{name}' failed to read {}: {err}",
                        resolved_path.display()
                    ))
                })?;
                values.insert(name.clone(), JsonValue::String(contents));
            }
            _ => {}
        }
    }

    let values_json = serde_json::to_string(&values)
        .map_err(|err| local_error(format!("failed to serialize template values: {err}")))?;
    Ok(ResolvedTemplateValues {
        values,
        values_json,
    })
}

fn stage_project(
    submit_root: &Path,
    project_root: &Path,
    config: &TemplateConfig,
    values: &BTreeMap<String, JsonValue>,
) -> AppResult<StagedProject> {
    let templated_files = resolve_templated_files(submit_root, project_root, &config.files)?;
    let temp_dir = TempDir::new()
        .map_err(|err| local_error(format!("failed to create staging directory: {err}")))?;
    let staging_root = temp_dir.path().to_path_buf();

    let context = Context::from_serialize(values)
        .map_err(|err| local_error(format!("failed to build template context: {err}")))?;

    for entry in WalkDir::new(submit_root).follow_links(false) {
        let entry =
            entry.map_err(|err| local_error(format!("failed to walk submit root: {err}")))?;
        if !entry.file_type().is_file() {
            continue;
        }
        let rel = entry
            .path()
            .strip_prefix(submit_root)
            .map_err(|_| local_error("failed to compute relative template path".to_string()))?;
        let dest = staging_root.join(rel);
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent).map_err(|err| {
                local_error(format!(
                    "failed to create staging directory {}: {err}",
                    parent.display()
                ))
            })?;
        }
        let rel_path = rel.to_path_buf();
        if templated_files.contains(&rel_path) {
            render_template_file(entry.path(), &dest, &context)?;
        } else {
            hard_link_or_copy(entry.path(), &dest)?;
        }
    }

    Ok(StagedProject {
        temp_dir,
        root: staging_root,
    })
}

fn resolve_templated_files(
    submit_root: &Path,
    project_root: &Path,
    files: &[String],
) -> AppResult<BTreeSet<PathBuf>> {
    let mut out = BTreeSet::new();
    for file in files {
        let trimmed = file.trim();
        if trimmed.is_empty() {
            return Err(invalid_argument("template file path cannot be empty"));
        }
        let relative = PathBuf::from(trimmed);
        if relative.is_absolute() {
            return Err(invalid_argument(format!(
                "template file '{trimmed}' must be relative to the project root"
            )));
        }
        let abs = project_root.join(&relative);
        if !abs.is_file() {
            return Err(invalid_argument(format!(
                "template file '{}' does not exist",
                abs.display()
            )));
        }
        let rel = abs.strip_prefix(submit_root).map_err(|_| {
            invalid_argument(format!(
                "template file '{}' is outside the submit root '{}'",
                abs.display(),
                submit_root.display()
            ))
        })?;
        out.insert(rel.to_path_buf());
    }
    Ok(out)
}

fn render_template_file(source: &Path, dest: &Path, context: &Context) -> AppResult<()> {
    let contents = std::fs::read_to_string(source).map_err(|err| {
        local_error(format!(
            "failed to read template {}: {err}",
            source.display()
        ))
    })?;
    let rendered = tera::Tera::one_off(&contents, context, false).map_err(|err| {
        invalid_argument(format!(
            "template render failed for {}: {err}",
            source.display()
        ))
    })?;
    std::fs::write(dest, rendered).map_err(|err| {
        local_error(format!(
            "failed to write template {}: {err}",
            dest.display()
        ))
    })?;
    copy_permissions(source, dest)?;
    Ok(())
}

fn hard_link_or_copy(source: &Path, dest: &Path) -> AppResult<()> {
    match std::fs::hard_link(source, dest) {
        Ok(()) => Ok(()),
        Err(_) => {
            std::fs::copy(source, dest).map_err(|err| {
                local_error(format!(
                    "failed to copy {} to {}: {err}",
                    source.display(),
                    dest.display()
                ))
            })?;
            copy_permissions(source, dest)
        }
    }
}

fn copy_permissions(source: &Path, dest: &Path) -> AppResult<()> {
    let permissions = std::fs::metadata(source)
        .map_err(|err| {
            local_error(format!(
                "failed to read permissions for {}: {err}",
                source.display()
            ))
        })?
        .permissions();
    std::fs::set_permissions(dest, permissions).map_err(|err| {
        local_error(format!(
            "failed to set permissions for {}: {err}",
            dest.display()
        ))
    })
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
        other => Err(invalid_argument(format!(
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
            _ => Err(invalid_argument(format!(
                "expected string, got {}",
                toml_type_name(value)
            ))),
        },
        TemplateFieldType::Integer => match value {
            toml::Value::Integer(v) => Ok(JsonValue::Number((*v).into())),
            toml::Value::Float(v) if v.fract() == 0.0 => Ok(JsonValue::Number((*v as i64).into())),
            _ => Err(invalid_argument(format!(
                "expected integer, got {}",
                toml_type_name(value)
            ))),
        },
        TemplateFieldType::Float => match value {
            toml::Value::Integer(v) => Ok(JsonValue::Number((*v).into())),
            toml::Value::Float(v) => serde_json::Number::from_f64(*v)
                .map(JsonValue::Number)
                .ok_or_else(|| invalid_argument("float value is not representable as JSON")),
            _ => Err(invalid_argument(format!(
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
                    Err(invalid_argument(format!(
                        "expected one of [{}]",
                        field.enum_values.join(", ")
                    )))
                }
            }
            _ => Err(invalid_argument(format!(
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
        return Err(invalid_argument(format!(
            "template field '{field_name}' has enum values but is not type 'enum'"
        )));
    }
    let mut out = Vec::new();
    let mut seen = BTreeSet::new();
    for value in values {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            return Err(invalid_argument(format!(
                "template field '{field_name}' has an empty enum value"
            )));
        }
        if !seen.insert(trimmed.to_string()) {
            return Err(invalid_argument(format!(
                "template field '{field_name}' has duplicate enum value '{trimmed}'"
            )));
        }
        out.push(trimmed.to_string());
    }
    if out.is_empty() {
        return Err(invalid_argument(format!(
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
            .ok_or_else(|| invalid_argument("float value is not representable as JSON")),
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

fn validate_template_field_name(name: &str) -> AppResult<()> {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return Err(invalid_argument("template field name cannot be empty"));
    };
    if !(first.is_ascii_alphabetic() || first == '_') {
        return Err(invalid_argument(format!(
            "template field name '{name}' must start with a letter or underscore"
        )));
    }
    if !chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_') {
        return Err(invalid_argument(format!(
            "template field name '{name}' must match ^[A-Za-z_][A-Za-z0-9_]*$"
        )));
    }
    Ok(())
}

fn validate_json_value(field: &TemplateField, value: &JsonValue) -> Result<(), String> {
    match field.field_type {
        TemplateFieldType::String
        | TemplateFieldType::FilePath
        | TemplateFieldType::FileContents => {
            if value.is_string() {
                Ok(())
            } else {
                Err("expected string".to_string())
            }
        }
        TemplateFieldType::Integer => {
            if value.as_i64().is_some() {
                Ok(())
            } else {
                Err("expected integer".to_string())
            }
        }
        TemplateFieldType::Float => {
            if value.as_f64().is_some() {
                Ok(())
            } else {
                Err("expected float".to_string())
            }
        }
        TemplateFieldType::Json => Ok(()),
        TemplateFieldType::Enum => {
            let Some(value) = value.as_str() else {
                return Err("expected string".to_string());
            };
            if field.enum_values.iter().any(|item| item == value) {
                Ok(())
            } else {
                Err(format!(
                    "expected one of [{}]",
                    field.enum_values.join(", ")
                ))
            }
        }
    }
}

fn is_path_field(field_type: TemplateFieldType) -> bool {
    matches!(
        field_type,
        TemplateFieldType::FilePath | TemplateFieldType::FileContents
    )
}

fn resolve_path(raw: &str, project_root: &Path) -> PathBuf {
    let path = PathBuf::from(raw);
    if path.is_absolute() {
        path
    } else {
        project_root.join(path)
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

fn invalid_argument(message: impl Into<String>) -> AppError {
    AppError::with_message(
        AppErrorKind::InvalidArgument,
        codes::INVALID_ARGUMENT,
        message,
    )
}

fn local_error(message: impl Into<String>) -> AppError {
    AppError::with_message(AppErrorKind::Internal, codes::LOCAL_ERROR, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prepare_template_submission_renders_files() {
        let root = tempfile::tempdir().expect("temp dir");
        let orbitfile = r#"
            [template]

            [template.fields.name]
            type = "string"

            [template.files]
            paths = ["config.txt"]
        "#;
        std::fs::write(root.path().join("Orbitfile"), orbitfile).expect("orbitfile");
        std::fs::write(root.path().join("config.txt"), "hello {{ name }}").expect("template file");

        let prepared = prepare_template_submission(root.path(), Some(r#"{ "name": "Ada" }"#))
            .expect("prepare")
            .expect("template");

        let rendered =
            std::fs::read_to_string(prepared.staging_root.join("config.txt")).expect("rendered");
        assert_eq!(rendered, "hello Ada");
    }
}
