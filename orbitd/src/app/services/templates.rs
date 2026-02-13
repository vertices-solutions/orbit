// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use tempfile::TempDir;
use tera::{Context, Template, ast};
use walkdir::WalkDir;

use crate::app::errors::{AppError, AppErrorKind, AppResult, codes};

const ORBITFILE_NAME: &str = "Orbitfile";
const SPECIAL_VARIABLE_PREFIX: &str = "ORBIT_";
const SPECIAL_SLURM_PARTITION: &str = "ORBIT_SLURM_PARTITION";
const SPECIAL_SLURM_ACCOUNT: &str = "ORBIT_SLURM_ACCOUNT";
const SPECIAL_SCRATCH_DIRECTORY: &str = "ORBIT_SCRATCH_DIRECTORY";

#[derive(Debug)]
pub struct PreparedTemplate {
    pub temp_dir: TempDir,
    pub staging_root: PathBuf,
    pub values_json: String,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
struct TemplateConfig {
    fields: BTreeMap<String, TemplateField>,
    files: Vec<String>,
    presets: BTreeMap<String, BTreeMap<String, JsonValue>>,
    #[serde(default)]
    special_variables: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum TemplateFieldType {
    String,
    Integer,
    Float,
    Json,
    FilePath,
    FileContents,
    Enum,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
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

    let templated_files =
        resolve_templated_files(submit_root, &project_root, &template_config.files)?;
    let special_variables =
        validate_template_field_references(&project_root, &templated_files, &template_config)?;
    let resolved_values = resolve_template_values(
        &template_config,
        template_values_json,
        &project_root,
        &special_variables,
    )?;
    let staged = stage_project(submit_root, &templated_files, &resolved_values.values)?;

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

pub fn load_template_config_json(orbitfile_path: &Path) -> AppResult<Option<String>> {
    let Some(mut config) = load_template_config(orbitfile_path)? else {
        return Ok(None);
    };
    let project_root = orbitfile_path.parent().ok_or_else(|| {
        local_error(format!(
            "failed to resolve Orbitfile parent directory for {}",
            orbitfile_path.display()
        ))
    })?;
    // Build-time validation should fail early when configured template targets are invalid.
    let templated_files = resolve_templated_files(project_root, project_root, &config.files)?;
    let special_variables =
        validate_template_field_references(project_root, &templated_files, &config)?;
    config.special_variables = special_variables.into_iter().collect();
    let json = serde_json::to_string(&config)
        .map_err(|err| local_error(format!("failed to serialize template config: {err}")))?;
    Ok(Some(json))
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
        special_variables: Vec::new(),
    }))
}

fn resolve_template_values(
    config: &TemplateConfig,
    template_values_json: Option<&str>,
    project_root: &Path,
    required_special_variables: &BTreeSet<String>,
) -> AppResult<ResolvedTemplateValues> {
    let mut values: BTreeMap<String, JsonValue> = BTreeMap::new();
    for (name, field) in &config.fields {
        if let Some(default) = field.default.as_ref() {
            values.insert(name.clone(), default.clone());
        }
    }

    for name in required_special_variables {
        if !is_known_special_variable(name) {
            return Err(invalid_argument(format!(
                "unknown special template variable '{name}'",
            )));
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
            if let Some(field) = config.fields.get(name) {
                validate_json_value(field, value).map_err(|err| {
                    invalid_argument(format!("template field '{name}' is invalid: {err}"))
                })?;
                values.insert(name.clone(), value.clone());
                continue;
            }
            if is_known_special_variable(name) {
                if value.as_str().is_none() {
                    return Err(invalid_argument(format!(
                        "special template variable '{name}' must be a string",
                    )));
                }
                values.insert(name.clone(), value.clone());
                continue;
            }
            if is_special_variable_name(name) {
                return Err(invalid_argument(format!(
                    "unknown special template variable '{name}'",
                )));
            }
            return Err(invalid_argument(format!(
                "template field '{name}' was not found",
            )));
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

    let mut missing_special = Vec::new();
    for name in required_special_variables {
        match values.get(name) {
            Some(value) => {
                if value.as_str().is_none() {
                    return Err(invalid_argument(format!(
                        "special template variable '{name}' must be a string",
                    )));
                }
            }
            None => missing_special.push(name.clone()),
        }
    }
    if !missing_special.is_empty() {
        return Err(invalid_argument(format!(
            "missing required special template variables: {}",
            missing_special.join(", ")
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
    templated_files: &BTreeSet<PathBuf>,
    values: &BTreeMap<String, JsonValue>,
) -> AppResult<StagedProject> {
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

fn validate_template_field_references(
    project_root: &Path,
    templated_files: &BTreeSet<PathBuf>,
    config: &TemplateConfig,
) -> AppResult<BTreeSet<String>> {
    // Build-time field validation works in three broad steps:
    // 1) Parse each configured template file into a Tera AST so we can inspect every expression.
    // 2) Walk the AST and collect root identifiers that are referenced but not declared as fields.
    // 3) Fail fast with a file-specific error when any undefined field is referenced.
    //
    // We intentionally perform this statically instead of rendering to catch issues in branches that
    // might not execute with a particular set of runtime values.
    let declared_fields: BTreeSet<String> = config.fields.keys().cloned().collect();
    let mut referenced_special_variables = BTreeSet::new();
    for rel_path in templated_files {
        let abs_path = project_root.join(rel_path);
        let content = std::fs::read_to_string(&abs_path).map_err(|err| {
            local_error(format!(
                "failed to read template {}: {err}",
                abs_path.display()
            ))
        })?;
        let template_name = rel_path.to_string_lossy().to_string();
        let template = Template::new(
            &template_name,
            Some(abs_path.display().to_string()),
            &content,
        )
        .map_err(|err| {
            invalid_argument(format!(
                "invalid template syntax in {}: {err}",
                abs_path.display()
            ))
        })?;

        let mut unknown = BTreeSet::new();
        let mut unknown_special = BTreeSet::new();
        let mut scopes = vec![BTreeSet::new()];
        collect_unknown_fields_from_nodes(
            &template.ast,
            &declared_fields,
            &mut scopes,
            &mut unknown,
            &mut referenced_special_variables,
            &mut unknown_special,
        );
        if !unknown_special.is_empty() {
            let names = unknown_special.into_iter().collect::<Vec<_>>().join(", ");
            return Err(invalid_argument(format!(
                "template file '{}' references unknown special template variables: {}",
                abs_path.display(),
                names
            )));
        }
        if !unknown.is_empty() {
            let names = unknown.into_iter().collect::<Vec<_>>().join(", ");
            return Err(invalid_argument(format!(
                "template file '{}' references undefined template fields: {}",
                abs_path.display(),
                names
            )));
        }
    }
    Ok(referenced_special_variables)
}

fn collect_unknown_fields_from_nodes(
    nodes: &[ast::Node],
    declared_fields: &BTreeSet<String>,
    scopes: &mut Vec<BTreeSet<String>>,
    unknown: &mut BTreeSet<String>,
    referenced_special: &mut BTreeSet<String>,
    unknown_special: &mut BTreeSet<String>,
) {
    // This is the recursive AST walker used by build-time validation.
    // It traverses every node, delegates expression analysis to helper functions, and maintains a
    // scope stack for locals introduced by template constructs (for example: macro args, `set`
    // bindings, loop variables, and `loop`) so those names are not mistaken for Orbitfile fields.
    for node in nodes {
        match node {
            ast::Node::VariableBlock(_, expr) => {
                collect_unknown_fields_from_expr(
                    expr,
                    declared_fields,
                    scopes,
                    unknown,
                    referenced_special,
                    unknown_special,
                );
            }
            ast::Node::MacroDefinition(_, definition, _) => {
                scopes.push(BTreeSet::new());
                if let Some(scope) = scopes.last_mut() {
                    for argument in definition.args.keys() {
                        scope.insert(argument.clone());
                    }
                }
                for default in definition.args.values().flatten() {
                    collect_unknown_fields_from_expr(
                        default,
                        declared_fields,
                        scopes,
                        unknown,
                        referenced_special,
                        unknown_special,
                    );
                }
                collect_unknown_fields_from_nodes(
                    &definition.body,
                    declared_fields,
                    scopes,
                    unknown,
                    referenced_special,
                    unknown_special,
                );
                scopes.pop();
            }
            ast::Node::Set(_, set) => {
                collect_unknown_fields_from_expr(
                    &set.value,
                    declared_fields,
                    scopes,
                    unknown,
                    referenced_special,
                    unknown_special,
                );
                register_scope_symbol(scopes, &set.key, set.global);
            }
            ast::Node::FilterSection(_, section, _) => {
                collect_unknown_fields_from_function_call(
                    &section.filter,
                    declared_fields,
                    scopes,
                    unknown,
                    referenced_special,
                    unknown_special,
                );
                scopes.push(BTreeSet::new());
                collect_unknown_fields_from_nodes(
                    &section.body,
                    declared_fields,
                    scopes,
                    unknown,
                    referenced_special,
                    unknown_special,
                );
                scopes.pop();
            }
            ast::Node::Block(_, block, _) => {
                scopes.push(BTreeSet::new());
                collect_unknown_fields_from_nodes(
                    &block.body,
                    declared_fields,
                    scopes,
                    unknown,
                    referenced_special,
                    unknown_special,
                );
                scopes.pop();
            }
            ast::Node::Forloop(_, forloop, _) => {
                collect_unknown_fields_from_expr(
                    &forloop.container,
                    declared_fields,
                    scopes,
                    unknown,
                    referenced_special,
                    unknown_special,
                );
                scopes.push(BTreeSet::new());
                register_scope_symbol(scopes, &forloop.value, false);
                if let Some(key) = &forloop.key {
                    register_scope_symbol(scopes, key, false);
                }
                register_scope_symbol(scopes, "loop", false);
                collect_unknown_fields_from_nodes(
                    &forloop.body,
                    declared_fields,
                    scopes,
                    unknown,
                    referenced_special,
                    unknown_special,
                );
                scopes.pop();

                if let Some(empty_body) = &forloop.empty_body {
                    scopes.push(BTreeSet::new());
                    collect_unknown_fields_from_nodes(
                        empty_body,
                        declared_fields,
                        scopes,
                        unknown,
                        referenced_special,
                        unknown_special,
                    );
                    scopes.pop();
                }
            }
            ast::Node::If(if_node, _) => {
                for (_, expr, body) in &if_node.conditions {
                    collect_unknown_fields_from_expr(
                        expr,
                        declared_fields,
                        scopes,
                        unknown,
                        referenced_special,
                        unknown_special,
                    );
                    scopes.push(BTreeSet::new());
                    collect_unknown_fields_from_nodes(
                        body,
                        declared_fields,
                        scopes,
                        unknown,
                        referenced_special,
                        unknown_special,
                    );
                    scopes.pop();
                }
                if let Some((_, body)) = &if_node.otherwise {
                    scopes.push(BTreeSet::new());
                    collect_unknown_fields_from_nodes(
                        body,
                        declared_fields,
                        scopes,
                        unknown,
                        referenced_special,
                        unknown_special,
                    );
                    scopes.pop();
                }
            }
            ast::Node::Super
            | ast::Node::Text(_)
            | ast::Node::Extends(_, _)
            | ast::Node::Include(_, _, _)
            | ast::Node::ImportMacro(_, _, _)
            | ast::Node::Raw(_, _, _)
            | ast::Node::Break(_)
            | ast::Node::Continue(_)
            | ast::Node::Comment(_, _) => {}
        }
    }
}

fn collect_unknown_fields_from_expr(
    expr: &ast::Expr,
    declared_fields: &BTreeSet<String>,
    scopes: &mut Vec<BTreeSet<String>>,
    unknown: &mut BTreeSet<String>,
    referenced_special: &mut BTreeSet<String>,
    unknown_special: &mut BTreeSet<String>,
) {
    collect_unknown_fields_from_expr_val(
        &expr.val,
        declared_fields,
        scopes,
        unknown,
        referenced_special,
        unknown_special,
    );
    for filter in &expr.filters {
        collect_unknown_fields_from_function_call(
            filter,
            declared_fields,
            scopes,
            unknown,
            referenced_special,
            unknown_special,
        );
    }
}

fn collect_unknown_fields_from_function_call(
    function_call: &ast::FunctionCall,
    declared_fields: &BTreeSet<String>,
    scopes: &mut Vec<BTreeSet<String>>,
    unknown: &mut BTreeSet<String>,
    referenced_special: &mut BTreeSet<String>,
    unknown_special: &mut BTreeSet<String>,
) {
    for argument in function_call.args.values() {
        collect_unknown_fields_from_expr(
            argument,
            declared_fields,
            scopes,
            unknown,
            referenced_special,
            unknown_special,
        );
    }
}

fn collect_unknown_fields_from_expr_val(
    value: &ast::ExprVal,
    declared_fields: &BTreeSet<String>,
    scopes: &mut Vec<BTreeSet<String>>,
    unknown: &mut BTreeSet<String>,
    referenced_special: &mut BTreeSet<String>,
    unknown_special: &mut BTreeSet<String>,
) {
    match value {
        ast::ExprVal::Ident(ident) => {
            maybe_add_unknown_field(
                ident,
                declared_fields,
                scopes,
                unknown,
                referenced_special,
                unknown_special,
            );
        }
        ast::ExprVal::Math(math_expr) => {
            collect_unknown_fields_from_expr(
                &math_expr.lhs,
                declared_fields,
                scopes,
                unknown,
                referenced_special,
                unknown_special,
            );
            collect_unknown_fields_from_expr(
                &math_expr.rhs,
                declared_fields,
                scopes,
                unknown,
                referenced_special,
                unknown_special,
            );
        }
        ast::ExprVal::Logic(logic_expr) => {
            collect_unknown_fields_from_expr(
                &logic_expr.lhs,
                declared_fields,
                scopes,
                unknown,
                referenced_special,
                unknown_special,
            );
            collect_unknown_fields_from_expr(
                &logic_expr.rhs,
                declared_fields,
                scopes,
                unknown,
                referenced_special,
                unknown_special,
            );
        }
        ast::ExprVal::Test(test) => {
            maybe_add_unknown_field(
                &test.ident,
                declared_fields,
                scopes,
                unknown,
                referenced_special,
                unknown_special,
            );
            for argument in &test.args {
                collect_unknown_fields_from_expr(
                    argument,
                    declared_fields,
                    scopes,
                    unknown,
                    referenced_special,
                    unknown_special,
                );
            }
        }
        ast::ExprVal::MacroCall(macro_call) => {
            for argument in macro_call.args.values() {
                collect_unknown_fields_from_expr(
                    argument,
                    declared_fields,
                    scopes,
                    unknown,
                    referenced_special,
                    unknown_special,
                );
            }
        }
        ast::ExprVal::FunctionCall(function_call) => {
            collect_unknown_fields_from_function_call(
                function_call,
                declared_fields,
                scopes,
                unknown,
                referenced_special,
                unknown_special,
            );
        }
        ast::ExprVal::Array(items) => {
            for item in items {
                collect_unknown_fields_from_expr(
                    item,
                    declared_fields,
                    scopes,
                    unknown,
                    referenced_special,
                    unknown_special,
                );
            }
        }
        ast::ExprVal::StringConcat(concat) => {
            for item in &concat.values {
                collect_unknown_fields_from_expr_val(
                    item,
                    declared_fields,
                    scopes,
                    unknown,
                    referenced_special,
                    unknown_special,
                );
            }
        }
        ast::ExprVal::In(expr_in) => {
            collect_unknown_fields_from_expr(
                &expr_in.lhs,
                declared_fields,
                scopes,
                unknown,
                referenced_special,
                unknown_special,
            );
            collect_unknown_fields_from_expr(
                &expr_in.rhs,
                declared_fields,
                scopes,
                unknown,
                referenced_special,
                unknown_special,
            );
        }
        ast::ExprVal::String(_)
        | ast::ExprVal::Int(_)
        | ast::ExprVal::Float(_)
        | ast::ExprVal::Bool(_) => {}
    }
}

fn maybe_add_unknown_field(
    ident: &str,
    declared_fields: &BTreeSet<String>,
    scopes: &[BTreeSet<String>],
    unknown: &mut BTreeSet<String>,
    referenced_special: &mut BTreeSet<String>,
    unknown_special: &mut BTreeSet<String>,
) {
    let Some(root) = ident_root(ident) else {
        return;
    };
    if is_known_special_variable(root) {
        referenced_special.insert(root.to_string());
        return;
    }
    if is_special_variable_name(root) {
        unknown_special.insert(root.to_string());
        return;
    }
    if declared_fields.contains(root) || scopes.iter().rev().any(|scope| scope.contains(root)) {
        return;
    }
    unknown.insert(root.to_string());
}

fn ident_root(ident: &str) -> Option<&str> {
    let trimmed = ident.trim();
    if trimmed.is_empty() {
        return None;
    }
    let split_idx = trimmed
        .find(|ch: char| ch == '.' || ch == '[')
        .unwrap_or(trimmed.len());
    if split_idx == 0 {
        None
    } else {
        Some(&trimmed[..split_idx])
    }
}

fn register_scope_symbol(scopes: &mut [BTreeSet<String>], symbol: &str, global: bool) {
    let trimmed = symbol.trim();
    if trimmed.is_empty() {
        return;
    }
    if global {
        if let Some(root_scope) = scopes.first_mut() {
            root_scope.insert(trimmed.to_string());
        }
    } else if let Some(current_scope) = scopes.last_mut() {
        current_scope.insert(trimmed.to_string());
    }
}

fn is_special_variable_name(name: &str) -> bool {
    name.trim_start().starts_with(SPECIAL_VARIABLE_PREFIX)
}

fn is_known_special_variable(name: &str) -> bool {
    matches!(
        name,
        SPECIAL_SLURM_PARTITION | SPECIAL_SLURM_ACCOUNT | SPECIAL_SCRATCH_DIRECTORY
    )
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

    #[test]
    fn load_template_config_json_rejects_missing_template_file() {
        let root = tempfile::tempdir().expect("temp dir");
        let orbitfile = r#"
            [template]

            [template.files]
            paths = ["config.txt"]
        "#;
        let orbitfile_path = root.path().join("Orbitfile");
        std::fs::write(&orbitfile_path, orbitfile).expect("orbitfile");

        let err = load_template_config_json(&orbitfile_path).expect_err("missing file should fail");
        assert_eq!(err.code(), codes::INVALID_ARGUMENT);
        assert!(err.message().contains("template file"));
        assert!(err.message().contains("does not exist"));
    }

    #[test]
    fn load_template_config_json_accepts_existing_template_file() {
        let root = tempfile::tempdir().expect("temp dir");
        let orbitfile = r#"
            [template]

            [template.files]
            paths = ["config.txt"]
        "#;
        let orbitfile_path = root.path().join("Orbitfile");
        std::fs::write(&orbitfile_path, orbitfile).expect("orbitfile");
        std::fs::write(root.path().join("config.txt"), "hello").expect("template file");

        let json = load_template_config_json(&orbitfile_path)
            .expect("load config")
            .expect("template config");
        assert!(json.contains("\"files\":[\"config.txt\"]"));
    }

    #[test]
    fn load_template_config_json_rejects_undeclared_template_variables() {
        let root = tempfile::tempdir().expect("temp dir");
        let orbitfile = r#"
            [template]

            [template.fields.name]
            type = "string"

            [template.files]
            paths = ["config.txt"]
        "#;
        let orbitfile_path = root.path().join("Orbitfile");
        std::fs::write(&orbitfile_path, orbitfile).expect("orbitfile");
        std::fs::write(
            root.path().join("config.txt"),
            "hello {{ name }} {{ cluster.name }}",
        )
        .expect("template file");

        let err =
            load_template_config_json(&orbitfile_path).expect_err("undeclared field should fail");
        assert_eq!(err.code(), codes::INVALID_ARGUMENT);
        assert!(err.message().contains("undefined template fields"));
        assert!(err.message().contains("cluster"));
    }

    #[test]
    fn load_template_config_json_allows_local_set_and_loop_variables() {
        let root = tempfile::tempdir().expect("temp dir");
        let orbitfile = r#"
            [template]

            [template.fields.name]
            type = "string"

            [template.fields.items]
            type = "json"

            [template.files]
            paths = ["config.txt"]
        "#;
        let orbitfile_path = root.path().join("Orbitfile");
        std::fs::write(&orbitfile_path, orbitfile).expect("orbitfile");
        std::fs::write(
            root.path().join("config.txt"),
            r#"{% set greeting = "hello" %}{{ greeting }} {{ name }}{% for item in items %} {{ item }}{% endfor %}"#,
        )
        .expect("template file");

        let json = load_template_config_json(&orbitfile_path)
            .expect("load config")
            .expect("template config");
        assert!(json.contains("\"name\""));
        assert!(json.contains("\"items\""));
    }

    #[test]
    fn load_template_config_json_checks_all_branches_for_undeclared_variables() {
        let root = tempfile::tempdir().expect("temp dir");
        let orbitfile = r#"
            [template]

            [template.files]
            paths = ["config.txt"]
        "#;
        let orbitfile_path = root.path().join("Orbitfile");
        std::fs::write(&orbitfile_path, orbitfile).expect("orbitfile");
        std::fs::write(
            root.path().join("config.txt"),
            "{% if false %}{{ missing_field }}{% endif %}",
        )
        .expect("template file");

        let err =
            load_template_config_json(&orbitfile_path).expect_err("missing field should fail");
        assert_eq!(err.code(), codes::INVALID_ARGUMENT);
        assert!(err.message().contains("missing_field"));
    }

    #[test]
    fn load_template_config_json_tracks_known_special_variables() {
        let root = tempfile::tempdir().expect("temp dir");
        let orbitfile = r#"
            [template]

            [template.files]
            paths = ["config.txt"]
        "#;
        let orbitfile_path = root.path().join("Orbitfile");
        std::fs::write(&orbitfile_path, orbitfile).expect("orbitfile");
        std::fs::write(
            root.path().join("config.txt"),
            "partition={{ ORBIT_SLURM_PARTITION }} scratch={{ ORBIT_SCRATCH_DIRECTORY }}",
        )
        .expect("template file");

        let json = load_template_config_json(&orbitfile_path)
            .expect("load config")
            .expect("template config");
        assert!(json.contains("ORBIT_SLURM_PARTITION"));
        assert!(json.contains("ORBIT_SCRATCH_DIRECTORY"));
    }

    #[test]
    fn load_template_config_json_rejects_unknown_special_variables() {
        let root = tempfile::tempdir().expect("temp dir");
        let orbitfile = r#"
            [template]

            [template.files]
            paths = ["config.txt"]
        "#;
        let orbitfile_path = root.path().join("Orbitfile");
        std::fs::write(&orbitfile_path, orbitfile).expect("orbitfile");
        std::fs::write(
            root.path().join("config.txt"),
            "account={{ ORBIT_UNKNOWN }}",
        )
        .expect("template file");

        let err =
            load_template_config_json(&orbitfile_path).expect_err("unknown special should fail");
        assert_eq!(err.code(), codes::INVALID_ARGUMENT);
        assert!(err.message().contains("unknown special template variables"));
        assert!(err.message().contains("ORBIT_UNKNOWN"));
    }

    #[test]
    fn load_template_config_json_rejects_orbit_nonexistent_variable() {
        let root = tempfile::tempdir().expect("temp dir");
        let orbitfile = r#"
            [template]

            [template.files]
            paths = ["config.txt"]
        "#;
        let orbitfile_path = root.path().join("Orbitfile");
        std::fs::write(&orbitfile_path, orbitfile).expect("orbitfile");
        std::fs::write(
            root.path().join("config.txt"),
            "value={{ ORBIT_NONEXISTENT }} another_value={{ORBIT_DEFAULT_PARTITION}}",
        )
        .expect("template file");

        let err = load_template_config_json(&orbitfile_path)
            .expect_err("ORBIT_NONEXISTENT should be rejected");
        assert_eq!(err.code(), codes::INVALID_ARGUMENT);
        assert!(err.message().contains("unknown special template variables"));
        assert!(err.message().contains("ORBIT_NONEXISTENT"));
    }

    #[test]
    fn prepare_template_submission_requires_referenced_special_values() {
        let root = tempfile::tempdir().expect("temp dir");
        let orbitfile = r#"
            [template]

            [template.files]
            paths = ["config.txt"]
        "#;
        std::fs::write(root.path().join("Orbitfile"), orbitfile).expect("orbitfile");
        std::fs::write(
            root.path().join("config.txt"),
            "partition={{ ORBIT_SLURM_PARTITION }}",
        )
        .expect("template file");

        let err = prepare_template_submission(root.path(), Some("{}"))
            .expect_err("missing special value should fail");
        assert_eq!(err.code(), codes::INVALID_ARGUMENT);
        assert!(
            err.message()
                .contains("missing required special template variables")
        );
        assert!(err.message().contains("ORBIT_SLURM_PARTITION"));
    }

    #[test]
    fn prepare_template_submission_renders_referenced_special_values() {
        let root = tempfile::tempdir().expect("temp dir");
        let orbitfile = r#"
            [template]

            [template.files]
            paths = ["config.txt"]
        "#;
        std::fs::write(root.path().join("Orbitfile"), orbitfile).expect("orbitfile");
        std::fs::write(
            root.path().join("config.txt"),
            "partition={{ ORBIT_SLURM_PARTITION }}",
        )
        .expect("template file");

        let prepared = prepare_template_submission(
            root.path(),
            Some(r#"{ "ORBIT_SLURM_PARTITION": "compute" }"#),
        )
        .expect("prepare")
        .expect("template");
        let rendered =
            std::fs::read_to_string(prepared.staging_root.join("config.txt")).expect("rendered");
        assert_eq!(rendered, "partition=compute");
    }
}
