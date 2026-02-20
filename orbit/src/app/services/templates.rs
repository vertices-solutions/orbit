// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use crossterm::terminal;
use serde_json::Value as JsonValue;

use crate::app::UiMode;
use crate::app::errors::{AppError, AppResult};
use crate::app::ports::{FilesystemPort, InteractionPort, OrbitdPort, OutputPort};
use crate::app::services::{TemplateConfig, TemplateFieldType};

pub type TemplateValues = BTreeMap<String, JsonValue>;

const SPECIAL_VARIABLE_PREFIX: &str = "ORBIT_";
const SPECIAL_SLURM_PARTITION: &str = "ORBIT_SLURM_PARTITION";
const SPECIAL_SLURM_ACCOUNT: &str = "ORBIT_SLURM_ACCOUNT";
const SPECIAL_SCRATCH_DIRECTORY: &str = "ORBIT_SCRATCH_DIRECTORY";

pub struct TemplateSpecialContext<'a> {
    pub cluster_name: &'a str,
    pub accounting_available: bool,
    pub default_scratch_directory: Option<&'a str>,
    pub orbitd: &'a dyn OrbitdPort,
}

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
    special_context: TemplateSpecialContext<'_>,
    validate_paths: bool,
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

    let mut special_overrides = BTreeMap::new();
    let overrides = parse_field_args(field_args)?;
    for (name, raw_value) in overrides {
        if is_special_variable_name(&name) {
            if !is_known_special_variable(&name) {
                return Err(AppError::invalid_argument(format!(
                    "unknown special template variable '{name}'",
                )));
            }
            special_overrides.insert(name, raw_value);
            continue;
        }
        let field = config.fields.get(&name).ok_or_else(|| {
            AppError::invalid_argument(format!("template field '{name}' was not found"))
        })?;
        let parsed =
            parse_template_value_from_string(field.field_type, &field.enum_values, &raw_value)?;
        values.insert(name, parsed);
    }
    let mut referenced_special_variables =
        collect_referenced_special_variables(config, project_root, fs, validate_paths)?;
    for name in special_overrides.keys() {
        referenced_special_variables.insert(name.clone());
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
                validate_paths,
            )
            .await?;
            values.insert(name.clone(), updated);
        }
    } else {
        for (name, field) in &config.fields {
            if let Some(value) = values.get(name)
                && validate_paths
                && is_path_field(field.field_type)
            {
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
                        validate_paths,
                    )
                    .await?;
                    values.insert(name.clone(), updated);
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
                validate_paths,
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
    }

    let special_values = resolve_special_template_values(
        &referenced_special_variables,
        &special_overrides,
        &special_context,
        interaction,
        output,
        ui_mode,
    )
    .await?;
    for (name, value) in special_values {
        let parsed = JsonValue::String(value.clone());
        let label = special_variable_label(&name);
        let message = format_confirmation_message(label, &parsed, Some(&value))?;
        output.success(&message).await?;
        values.insert(name, parsed);
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
    validate_paths: bool,
) -> AppResult<JsonValue> {
    if field_type == TemplateFieldType::Enum {
        if enum_values.is_empty() {
            return Err(AppError::invalid_argument(format!(
                "template field '{name}' has no enum values"
            )));
        }
        let default_value = default
            .and_then(|value| value.as_str())
            .map(|v| v.to_string());
        let selected = interaction
            .select_enum(
                name,
                enum_values,
                default_value.as_deref(),
                description.unwrap_or(""),
            )
            .await?;
        let parsed = JsonValue::String(selected.clone());
        let message = format_confirmation_message(name, &parsed, Some(&selected))?;
        output.success(&message).await?;
        return Ok(parsed);
    }

    let prompt = format!("{name}: ");
    let help = description.unwrap_or("");
    loop {
        let mut prompt_line = if let Some(default) = default {
            let default_str = json_value_to_string(default)?;
            interaction
                .prompt_line_with_default_confirmable(&prompt, help, &default_str)
                .await?
        } else {
            interaction.prompt_line_confirmable(&prompt, help).await?
        };
        let input = prompt_line.input.clone();
        prompt_line.start_validation("Validating input...")?;
        let parsed = match parse_template_value_from_string(field_type, enum_values, &input) {
            Ok(value) => value,
            Err(err) => {
                prompt_line.finish_failure(&validation_error_message(&err))?;
                continue;
            }
        };
        if validate_paths && is_path_field(field_type) {
            if let Err(err) = validate_path(&input, project_root, field_type, fs) {
                prompt_line.finish_failure(&validation_error_message(&err))?;
                continue;
            }
        }
        let message = format_confirmation_message(name, &parsed, Some(&input))?;
        prompt_line.finish_success(&message)?;
        return Ok(parsed);
    }
}

fn validation_error_message(err: &AppError) -> String {
    if err.message.trim().is_empty() {
        "validation failed".to_string()
    } else {
        err.message.clone()
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

fn collect_referenced_special_variables(
    config: &TemplateConfig,
    project_root: &Path,
    fs: &dyn FilesystemPort,
    validate_paths: bool,
) -> AppResult<BTreeSet<String>> {
    let mut out = BTreeSet::new();
    if !config.special_variables.is_empty() {
        for name in &config.special_variables {
            let trimmed = name.trim();
            if trimmed.is_empty() {
                continue;
            }
            if !is_special_variable_name(trimmed) {
                continue;
            }
            if !is_known_special_variable(trimmed) {
                return Err(AppError::invalid_argument(format!(
                    "unknown special template variable '{trimmed}'",
                )));
            }
            out.insert(trimmed.to_string());
        }
        return Ok(out);
    }

    for relative in &config.files {
        let trimmed = relative.trim();
        if trimmed.is_empty() {
            continue;
        }
        let path = resolve_path(trimmed, project_root);
        let bytes = match fs.read_file(&path) {
            Ok(bytes) => bytes,
            Err(err) => {
                if validate_paths {
                    return Err(AppError::invalid_argument(format!(
                        "failed to read template file '{}': {}",
                        path.display(),
                        err.message
                    )));
                }
                continue;
            }
        };
        let content = match String::from_utf8(bytes) {
            Ok(content) => content,
            Err(err) => {
                if validate_paths {
                    return Err(AppError::invalid_argument(format!(
                        "template file '{}' is not valid UTF-8: {err}",
                        path.display()
                    )));
                }
                continue;
            }
        };
        for token in extract_special_tokens(&content) {
            if !is_known_special_variable(&token) {
                return Err(AppError::invalid_argument(format!(
                    "template file '{}' references unknown special template variable '{}'",
                    path.display(),
                    token
                )));
            }
            out.insert(token);
        }
    }

    Ok(out)
}

fn extract_special_tokens(content: &str) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    let bytes = content.as_bytes();
    let mut idx = 0usize;
    while idx < bytes.len() {
        let Some(found) = content[idx..].find(SPECIAL_VARIABLE_PREFIX) else {
            break;
        };
        let start = idx + found;
        if start > 0 {
            let prev = bytes[start - 1] as char;
            if prev.is_ascii_alphanumeric() || prev == '_' {
                idx = start + 1;
                continue;
            }
        }
        let mut end = start + SPECIAL_VARIABLE_PREFIX.len();
        while end < bytes.len() {
            let ch = bytes[end] as char;
            if ch.is_ascii_alphanumeric() || ch == '_' {
                end += 1;
            } else {
                break;
            }
        }
        if end > start + SPECIAL_VARIABLE_PREFIX.len() {
            out.insert(content[start..end].to_string());
        }
        idx = end;
    }
    out
}

async fn resolve_special_template_values(
    names: &BTreeSet<String>,
    overrides: &BTreeMap<String, String>,
    context: &TemplateSpecialContext<'_>,
    interaction: &dyn InteractionPort,
    output: &dyn OutputPort,
    ui_mode: UiMode,
) -> AppResult<BTreeMap<String, String>> {
    let mut out = BTreeMap::new();
    for name in names {
        if let Some(value) = overrides.get(name) {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                return Err(AppError::invalid_argument(format!(
                    "special template variable '{name}' cannot be empty",
                )));
            }
            out.insert(name.clone(), trimmed.to_string());
            continue;
        }
        let resolved =
            resolve_special_template_value(name, context, interaction, output, ui_mode).await?;
        out.insert(name.clone(), resolved);
    }
    Ok(out)
}

async fn resolve_special_template_value(
    name: &str,
    context: &TemplateSpecialContext<'_>,
    interaction: &dyn InteractionPort,
    output: &dyn OutputPort,
    ui_mode: UiMode,
) -> AppResult<String> {
    match name {
        SPECIAL_SLURM_PARTITION => {
            resolve_partition_value(context, interaction, output, ui_mode).await
        }
        SPECIAL_SLURM_ACCOUNT => resolve_account_value(context, interaction, output, ui_mode).await,
        SPECIAL_SCRATCH_DIRECTORY => {
            resolve_scratch_directory_value(context, interaction, output, ui_mode).await
        }
        _ => Err(AppError::invalid_argument(format!(
            "unknown special template variable '{name}'",
        ))),
    }
}

async fn resolve_partition_value(
    context: &TemplateSpecialContext<'_>,
    interaction: &dyn InteractionPort,
    output: &dyn OutputPort,
    ui_mode: UiMode,
) -> AppResult<String> {
    let partitions =
        normalize_select_options(context.orbitd.list_partitions(context.cluster_name).await?);
    if partitions.is_empty() {
        output
            .warn(&format!(
                "No Slurm partitions were discovered on cluster '{}'. Enter a partition manually.",
                context.cluster_name
            ))
            .await?;
        return prompt_manual_special_value(
            SPECIAL_SLURM_PARTITION,
            "Slurm partition: ",
            "Partition name used for '--partition'.",
            interaction,
            output,
            ui_mode,
        )
        .await;
    }
    if !ui_mode.is_interactive() {
        return Err(non_interactive_special_error(SPECIAL_SLURM_PARTITION));
    }
    interaction
        .select_enum(
            "Slurm partition",
            &partitions,
            partitions.first().map(String::as_str),
            "Select a Slurm partition for this submission.",
        )
        .await
}

async fn resolve_account_value(
    context: &TemplateSpecialContext<'_>,
    interaction: &dyn InteractionPort,
    output: &dyn OutputPort,
    ui_mode: UiMode,
) -> AppResult<String> {
    if !context.accounting_available {
        output
            .warn(&format!(
                "Slurm accounting is disabled on cluster '{}'. Entering a Slurm account likely won't have any effects on scheduling.",
                context.cluster_name
            ))
            .await?;
        return prompt_manual_special_value(
            SPECIAL_SLURM_ACCOUNT,
            "Slurm account: ",
            "Account value to inject into templated files.",
            interaction,
            output,
            ui_mode,
        )
        .await;
    }

    let accounts =
        normalize_select_options(context.orbitd.list_accounts(context.cluster_name).await?);
    if accounts.is_empty() {
        output
            .warn(&format!(
                "No Slurm accounts were discovered on cluster '{}'. Enter an account manually.",
                context.cluster_name
            ))
            .await?;
        return prompt_manual_special_value(
            SPECIAL_SLURM_ACCOUNT,
            "Slurm account: ",
            "Account value to inject into templated files.",
            interaction,
            output,
            ui_mode,
        )
        .await;
    }
    if !ui_mode.is_interactive() {
        return Err(non_interactive_special_error(SPECIAL_SLURM_ACCOUNT));
    }
    interaction
        .select_enum(
            "Slurm account",
            &accounts,
            accounts.first().map(String::as_str),
            "Select a Slurm account for this submission.",
        )
        .await
}

async fn resolve_scratch_directory_value(
    context: &TemplateSpecialContext<'_>,
    interaction: &dyn InteractionPort,
    output: &dyn OutputPort,
    ui_mode: UiMode,
) -> AppResult<String> {
    if let Some(value) = context
        .default_scratch_directory
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return Ok(value.to_string());
    }
    output
        .warn(&format!(
            "Cluster '{}' has no default scratch directory configured. Enter a scratch directory manually.",
            context.cluster_name
        ))
        .await?;
    prompt_manual_special_value(
        SPECIAL_SCRATCH_DIRECTORY,
        "Scratch directory: ",
        "Cluster scratch directory path.",
        interaction,
        output,
        ui_mode,
    )
    .await
}

fn normalize_select_options(values: Vec<String>) -> Vec<String> {
    let mut unique = BTreeSet::new();
    for value in values {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            continue;
        }
        unique.insert(trimmed.to_string());
    }
    unique.into_iter().collect()
}

async fn prompt_manual_special_value(
    name: &str,
    prompt: &str,
    help: &str,
    interaction: &dyn InteractionPort,
    _output: &dyn OutputPort,
    ui_mode: UiMode,
) -> AppResult<String> {
    if !ui_mode.is_interactive() {
        return Err(non_interactive_special_error(name));
    }
    loop {
        let mut prompt_line = interaction.prompt_line_confirmable(prompt, help).await?;
        let input = prompt_line.input.trim().to_string();
        prompt_line.start_validation("Validating input...")?;
        if input.is_empty() {
            prompt_line.finish_failure("value cannot be empty")?;
            continue;
        }
        prompt_line.stop_validation()?;
        return Ok(input);
    }
}

fn non_interactive_special_error(name: &str) -> AppError {
    AppError::invalid_argument(format!(
        "special template variable '{name}' requires interactive input; pass --field {name}=VALUE",
    ))
}

fn special_variable_label(name: &str) -> &str {
    match name {
        SPECIAL_SLURM_PARTITION => "Slurm partition",
        SPECIAL_SLURM_ACCOUNT => "Slurm account",
        SPECIAL_SCRATCH_DIRECTORY => "Scratch directory",
        _ => name,
    }
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
            let value = raw
                .trim()
                .parse::<f64>()
                .map_err(|_| AppError::invalid_argument(format!("expected float, got '{raw}'")))?;
            serde_json::Number::from_f64(value)
                .map(JsonValue::Number)
                .ok_or_else(|| AppError::invalid_argument("float value is not representable"))
        }
        TemplateFieldType::Json => serde_json::from_str(raw)
            .map_err(|err| AppError::invalid_argument(format!("invalid json value: {err}"))),
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

fn is_special_variable_name(name: &str) -> bool {
    name.trim().starts_with(SPECIAL_VARIABLE_PREFIX)
}

fn is_known_special_variable(name: &str) -> bool {
    matches!(
        name,
        SPECIAL_SLURM_PARTITION | SPECIAL_SLURM_ACCOUNT | SPECIAL_SCRATCH_DIRECTORY
    )
}
