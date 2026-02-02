// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

use std::path::{Path, PathBuf};

use proto::{SubmitPathFilterAction, SubmitPathFilterRule};
use serde::{Deserialize, Serialize};

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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrbitfileProjectConfig {
    pub root: PathBuf,
    pub name: String,
    pub default_retrieve_path: Option<String>,
    pub submit_sbatch_script: Option<String>,
    pub rules: ProjectRuleSet,
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
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '-'
            }
        })
        .collect::<String>()
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

    let project = raw
        .project
        .ok_or_else(|| AppError::invalid_argument("Orbitfile is missing [project]"))?;
    let name = project.name.trim().to_string();
    validate_project_name(&name)?;

    let default_retrieve_path =
        trim_optional(raw.retrieve.and_then(|section| section.default_path))
            .map(|value| value.to_string());
    let submit_sbatch_script = trim_optional(raw.submit.and_then(|section| section.sbatch_script))
        .map(|value| value.to_string());

    let mut include = Vec::new();
    let mut exclude = Vec::new();
    if let Some(sync) = raw.sync {
        include = normalize_patterns(sync.include, "sync.include")?;
        exclude = normalize_patterns(sync.exclude, "sync.exclude")?;
    }

    Ok(OrbitfileProjectConfig {
        root: project_root.to_path_buf(),
        name,
        default_retrieve_path,
        submit_sbatch_script,
        rules: ProjectRuleSet { include, exclude },
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

    if config.name != name {
        return CheckedProjectStatus {
            name: name.to_string(),
            ok: false,
            reason: Some(format!(
                "Project {} has invalid Orbitfile: [project].name '{}' does not match registry name '{}'",
                name, config.name, name
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

#[cfg(test)]
mod tests {
    use super::*;

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
    }
}
