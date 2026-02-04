// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crossterm::terminal;
use serde_json::Value as JsonValue;

use crate::app::UiMode;
use crate::app::errors::{AppError, AppResult};
use crate::app::ports::{FilesystemPort, InteractionPort, OutputPort};
use crate::app::services::{TemplateConfig, TemplateFieldType};

pub type TemplateValues = BTreeMap<String, JsonValue>;

pub async fn resolve_template_values(
    config: &TemplateConfig,
    preset: Option<&str>,
    field_args: &[String],
    fill_defaults: bool,
    project_root: &Path,
    fs: &dyn FilesystemPort,
    interaction: &dyn InteractionPort,
    output: &dyn OutputPort,
    ui_mode: UiMode,
) -> AppResult<TemplateValues> {
    let mut values: TemplateValues = BTreeMap::new();

    for (name, field) in &config.fields {
        if let Some(default) = field.default.as_ref() {
            values.insert(name.clone(), default.clone());
        }
    }

    if let Some(preset_name) = preset {
        let preset_values = config.presets.get(preset_name).ok_or_else(|| {
            AppError::invalid_argument(format!("template preset '{preset_name}' was not found"))
        })?;
        for (name, value) in preset_values {
            values.insert(name.clone(), value.clone());
        }
    }

    let overrides = parse_field_args(field_args)?;
    for (name, raw_value) in overrides {
        let field = config.fields.get(&name).ok_or_else(|| {
            AppError::invalid_argument(format!("template field '{name}' was not found"))
        })?;
        let parsed =
            parse_template_value_from_string(field.field_type, &field.enum_values, &raw_value)?;
        values.insert(name, parsed);
    }

    let interactive = ui_mode.is_interactive();
    let confirm_all = interactive && !fill_defaults;

    if confirm_all {
        for (name, field) in &config.fields {
            let updated = prompt_for_field(
                name,
                field.field_type,
                &field.enum_values,
                field.description.as_deref(),
                values.get(name),
                project_root,
                fs,
                interaction,
                output,
            )
            .await?;
            values.insert(name.clone(), updated);
        }
        return Ok(values);
    }

    for (name, field) in &config.fields {
        if let Some(value) = values.get(name) {
            if is_path_field(field.field_type) {
                let raw = value.as_str().ok_or_else(|| {
                    AppError::invalid_argument(format!(
                        "template field '{name}' must be a string path"
                    ))
                })?;
                if validate_path(raw, project_root, field.field_type, fs).is_err() {
                    if !interactive {
                        return Err(AppError::invalid_argument(format!(
                            "template field '{name}' path does not exist"
                        )));
                    }
                    let updated = prompt_for_field(
                        name,
                        field.field_type,
                        &field.enum_values,
                        field.description.as_deref(),
                        Some(value),
                        project_root,
                        fs,
                        interaction,
                        output,
                    )
                    .await?;
                    values.insert(name.clone(), updated);
                }
            }
        }
    }

    let mut missing = Vec::new();
    for (name, field) in &config.fields {
        if values.contains_key(name) {
            continue;
        }
        if !interactive {
            missing.push(name.clone());
            continue;
        }
        let updated = prompt_for_field(
            name,
            field.field_type,
            &field.enum_values,
            field.description.as_deref(),
            field.default.as_ref(),
            project_root,
            fs,
            interaction,
            output,
        )
        .await?;
        values.insert(name.clone(), updated);
    }
    if !missing.is_empty() {
        return Err(AppError::invalid_argument(format!(
            "missing required template fields: {}",
            missing.join(", ")
        )));
    }

    Ok(values)
}

async fn prompt_for_field(
    name: &str,
    field_type: TemplateFieldType,
    enum_values: &[String],
    description: Option<&str>,
    default: Option<&JsonValue>,
    project_root: &Path,
    fs: &dyn FilesystemPort,
    interaction: &dyn InteractionPort,
    output: &dyn OutputPort,
) -> AppResult<JsonValue> {
    if field_type == TemplateFieldType::Enum {
        if enum_values.is_empty() {
            return Err(AppError::invalid_argument(format!(
                "template field '{name}' has no enum values"
            )));
        }
        let default_value = default.and_then(|value| value.as_str()).map(|v| v.to_string());
        let selected = interaction
            .select_enum(name, enum_values, default_value.as_deref(), description.unwrap_or(""))
            .await?;
        let parsed = JsonValue::String(selected.clone());
        let message = format_confirmation_message(name, &parsed, Some(&selected))?;
        output.success(&message).await?;
        return Ok(parsed);
    }

    let prompt = format!("{name}: ");
    let help = description.unwrap_or("");
    loop {
        let input = if let Some(default) = default {
            let default_str = json_value_to_string(default)?;
            interaction
                .prompt_line_with_default(&prompt, help, &default_str)
                .await?
        } else {
            interaction.prompt_line(&prompt, help).await?
        };
        let parsed = match parse_template_value_from_string(field_type, enum_values, &input) {
            Ok(value) => value,
            Err(err) => {
                output.warn(&err.message).await?;
                continue;
            }
        };
        if is_path_field(field_type) {
            if let Err(err) = validate_path(&input, project_root, field_type, fs) {
                output.warn(&err.message).await?;
                continue;
            }
        }
        let message = format_confirmation_message(name, &parsed, Some(&input))?;
        output.success(&message).await?;
        return Ok(parsed);
    }
}

fn parse_field_args(field_args: &[String]) -> AppResult<BTreeMap<String, String>> {
    let mut out = BTreeMap::new();
    for arg in field_args {
        let mut iter = arg.splitn(2, '=');
        let key = iter.next().unwrap_or("").trim();
        let value = iter.next();
        if key.is_empty() || value.is_none() {
            return Err(AppError::invalid_argument(format!(
                "invalid field override '{arg}', expected KEY=VALUE"
            )));
        }
        out.insert(key.to_string(), value.unwrap().to_string());
    }
    Ok(out)
}

fn parse_template_value_from_string(
    field_type: TemplateFieldType,
    enum_values: &[String],
    raw: &str,
) -> AppResult<JsonValue> {
    match field_type {
        TemplateFieldType::String
        | TemplateFieldType::FilePath
        | TemplateFieldType::FileContents => Ok(JsonValue::String(raw.to_string())),
        TemplateFieldType::Integer => {
            let value = raw.trim().parse::<i64>().map_err(|_| {
                AppError::invalid_argument(format!("expected integer, got '{raw}'"))
            })?;
            Ok(JsonValue::Number(value.into()))
        }
        TemplateFieldType::Float => {
            let value = raw.trim().parse::<f64>().map_err(|_| {
                AppError::invalid_argument(format!("expected float, got '{raw}'"))
            })?;
            serde_json::Number::from_f64(value)
                .map(JsonValue::Number)
                .ok_or_else(|| AppError::invalid_argument("float value is not representable"))
        }
        TemplateFieldType::Json => serde_json::from_str(raw).map_err(|err| {
            AppError::invalid_argument(format!("invalid json value: {err}"))
        }),
        TemplateFieldType::Enum => {
            let trimmed = raw.trim();
            if enum_values.iter().any(|value| value == trimmed) {
                Ok(JsonValue::String(trimmed.to_string()))
            } else {
                Err(AppError::invalid_argument(format!(
                    "expected one of [{}]",
                    enum_values.join(", ")
                )))
            }
        }
    }
}

fn validate_path(
    raw: &str,
    project_root: &Path,
    field_type: TemplateFieldType,
    fs: &dyn FilesystemPort,
) -> AppResult<()> {
    if raw.trim().is_empty() {
        return Err(AppError::invalid_argument("path cannot be empty"));
    }
    let resolved = resolve_path(raw, project_root);
    match field_type {
        TemplateFieldType::FileContents => {
            if !fs.is_file(&resolved)? {
                return Err(AppError::invalid_argument(format!(
                    "file '{}' does not exist",
                    resolved.display()
                )));
            }
        }
        TemplateFieldType::FilePath => {
            if !fs.is_file(&resolved)? && !fs.is_dir(&resolved)? {
                return Err(AppError::invalid_argument(format!(
                    "path '{}' does not exist",
                    resolved.display()
                )));
            }
        }
        _ => {}
    }
    Ok(())
}

fn resolve_path(raw: &str, project_root: &Path) -> PathBuf {
    let path = PathBuf::from(raw);
    if path.is_absolute() {
        path
    } else {
        project_root.join(path)
    }
}

fn json_value_to_string(value: &JsonValue) -> AppResult<String> {
    match value {
        JsonValue::String(v) => Ok(v.clone()),
        JsonValue::Number(v) => Ok(v.to_string()),
        JsonValue::Bool(v) => Ok(v.to_string()),
        JsonValue::Array(_) | JsonValue::Object(_) | JsonValue::Null => {
            serde_json::to_string(value)
                .map_err(|err| AppError::invalid_argument(format!("invalid default: {err}")))
        }
    }
}

fn format_confirmation_message(
    name: &str,
    value: &JsonValue,
    raw_input: Option<&str>,
) -> AppResult<String> {
    let prefix = format!("{name}: ");
    let (mut display_value, value_len) = match value {
        JsonValue::Number(number) => {
            let rendered = number.to_string();
            let len = rendered.chars().count();
            (rendered, len)
        }
        _ => {
            let rendered = match raw_input {
                Some(raw) => raw.to_string(),
                None => match value {
                    JsonValue::String(text) => text.clone(),
                    _ => json_value_to_string(value)?,
                },
            };
            let len = rendered.chars().count();
            (rendered, len)
        }
    };
    if !matches!(value, JsonValue::Number(_)) {
        let width = terminal::size()
            .map(|(cols, _)| cols as usize)
            .unwrap_or(80);
        if width > 0 {
            let total_len = prefix.chars().count() + value_len;
            if total_len > width {
                display_value = format!("<{} characters>", value_len);
            }
        }
    }
    Ok(format!("{prefix}{display_value}"))
}

fn is_path_field(field_type: TemplateFieldType) -> bool {
    matches!(
        field_type,
        TemplateFieldType::FilePath | TemplateFieldType::FileContents
    )
}
