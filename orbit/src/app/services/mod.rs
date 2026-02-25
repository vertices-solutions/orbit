// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

use std::collections::HashSet;
use std::net::IpAddr;
use std::path::{Path, PathBuf};

use rand::prelude::IndexedRandom;

use crate::app::UiMode;
use crate::app::commands::AddClusterCommand;
use crate::app::errors::{AppError, AppResult};
use crate::app::ports::{FilesystemPort, InteractionPort, NetworkPort};

mod project;
mod templates;
pub use project::{
    BlueprintRuleSet, OrbitfileProjectConfig, TemplateConfig, TemplateField, TemplateFieldType,
    build_default_orbitfile_contents, discover_project_from_run_root, load_blueprint_from_root,
    load_project_from_root, merge_run_filters, resolve_orbitfile_sbatch_script,
    sanitize_blueprint_name, sanitize_project_name, template_config_from_json,
    upsert_orbitfile_blueprint_name, upsert_orbitfile_project_name, validate_blueprint_name,
    validate_project_name,
};
pub use templates::{TemplateSpecialContext, TemplateValues, resolve_template_values};

const DEFAULT_SSH_PORT: u32 = 22;
const DEFAULT_BASE_PATH: &str = "~/runs";
const OTHER_IDENTITY_OPTION: &str = "other";

#[derive(Debug, Clone)]
pub struct ResolvedAddCluster {
    pub hostname: Option<String>,
    pub ip: Option<String>,
    pub username: String,
    pub name: String,
    pub port: u32,
    pub identity_path: String,
    pub default_base_path: Option<String>,
    pub default_scratch_directory: Option<String>,
}

pub struct AddClusterResolver<'a> {
    interaction: &'a dyn InteractionPort,
    fs: &'a dyn FilesystemPort,
    network: &'a dyn NetworkPort,
    ui_mode: UiMode,
}

impl<'a> AddClusterResolver<'a> {
    pub fn new(
        interaction: &'a dyn InteractionPort,
        fs: &'a dyn FilesystemPort,
        network: &'a dyn NetworkPort,
        ui_mode: UiMode,
    ) -> Self {
        Self {
            interaction,
            fs,
            network,
            ui_mode,
        }
    }

    pub async fn resolve(
        &self,
        args: AddClusterCommand,
        existing_names: &HashSet<String>,
    ) -> AppResult<ResolvedAddCluster> {
        let non_interactive = !self.ui_mode.is_interactive();
        let destination = normalize_option(args.destination);
        let name_arg = normalize_option(args.name);
        let identity_path_arg = normalize_option(args.identity_path);
        let default_base_path_arg = normalize_option(args.default_base_path);

        let parsed_destination = if let Some(value) = destination.as_deref() {
            let parsed = parse_destination(value)?;
            self.validate_destination(&parsed)?;
            parsed
        } else {
            if non_interactive {
                return Err(AppError::invalid_argument(
                    "destination is required in non-interactive mode",
                ));
            }
            let (parsed, _) = self
                .prompt_and_validate(
                    "Destination: ",
                    "SSH destination in user@host[:port] form.",
                    None,
                    "Validating destination...",
                    |input| {
                        let parsed = parse_destination(input)?;
                        self.validate_destination(&parsed)?;
                        Ok((parsed, input.to_string()))
                    },
                    |(_, input)| format!("Destination: {input}"),
                )
                .await?;
            parsed
        };

        let (hostname, ip) = if parsed_destination.host.parse::<IpAddr>().is_ok() {
            (None, Some(parsed_destination.host.clone()))
        } else {
            (Some(parsed_destination.host.clone()), None)
        };

        let username = match parsed_destination.username.clone() {
            Some(value) => value,
            None => {
                return Err(AppError::invalid_argument(
                    "destination username is required",
                ));
            }
        };

        let mut name = match name_arg.as_deref() {
            Some(value) => value.to_string(),
            None => {
                if non_interactive {
                    default_name_for_host(hostname.as_deref(), ip.as_deref())?
                } else {
                    let default_name = generate_random_name();
                    self.prompt_name(existing_names, &default_name).await?
                }
            }
        };

        if non_interactive {
            validate_name(&name, existing_names)?;
        } else if name_arg.is_some() {
            match self
                .validate_with_feedback(
                    &name,
                    "Validating name...",
                    |input| {
                        validate_name(input, existing_names)?;
                        Ok(input.to_string())
                    },
                    |validated_name| format!("Name: {validated_name}"),
                )
                .await
            {
                Ok(validated_name) => {
                    name = validated_name;
                }
                Err(_) => {
                    name = self.prompt_name(existing_names, &name).await?;
                }
            }
        }

        let port = parsed_destination.port.unwrap_or(DEFAULT_SSH_PORT);

        let discovered_identity = find_preferred_identity_path(self.fs);
        let identity_path = match identity_path_arg {
            Some(value) => value,
            None => {
                if non_interactive {
                    match discovered_identity.as_deref() {
                        Some(value) => value.to_string(),
                        None => {
                            return Err(AppError::invalid_argument(
                                "identity path is required in non-interactive mode",
                            ));
                        }
                    }
                } else {
                    self.prompt_identity_path(discovered_identity.as_deref())
                        .await?
                }
            }
        };

        let identity_path = match validate_identity_path(self.fs, &identity_path) {
            Ok(()) => identity_path,
            Err(err) => {
                if non_interactive {
                    return Err(err);
                }
                self.prompt_identity_path(Some(identity_path.as_str()))
                    .await?
            }
        };

        let default_base_path = match default_base_path_arg {
            Some(value) => {
                if non_interactive {
                    local_validate_default_base_path(&value)?;
                }
                Some(value)
            }
            None => {
                if non_interactive {
                    Some(DEFAULT_BASE_PATH.to_string())
                } else {
                    None
                }
            }
        };

        Ok(ResolvedAddCluster {
            hostname,
            ip,
            username,
            name,
            port,
            identity_path,
            default_base_path,
            default_scratch_directory: None,
        })
    }

    fn validate_destination(&self, destination: &ParsedDestination) -> AppResult<()> {
        let port = resolve_destination_port(destination.port)?;
        self.network.check_reachable(&destination.host, port)?;
        Ok(())
    }

    async fn prompt_name(
        &self,
        existing_names: &HashSet<String>,
        default: &str,
    ) -> AppResult<String> {
        self.prompt_and_validate(
            "Name: ",
            "Friendly name used in other commands (e.g. gpu01).",
            Some(default),
            "Validating name...",
            |input| {
                validate_name(input, existing_names)?;
                Ok(input.to_string())
            },
            |name| format!("Name: {name}"),
        )
        .await
    }

    async fn prompt_identity_path(&self, default: Option<&str>) -> AppResult<String> {
        let identities = discover_identity_paths(self.fs);
        let (options, default_selection) = build_identity_selector_options(identities);
        let selected = self
            .interaction
            .select_enum(
                "SSH key",
                &options,
                Some(default_selection.as_str()),
                "Select a private key from ~/.ssh, or choose other to enter a custom path.",
            )
            .await?;
        if selected == OTHER_IDENTITY_OPTION {
            return self.prompt_custom_identity_path(default).await;
        }
        match self
            .validate_with_feedback(
                &selected,
                "Validating identity path...",
                |input| {
                    validate_identity_path(self.fs, input)?;
                    Ok(input.to_string())
                },
                |path| format!("Identity path: {path}"),
            )
            .await
        {
            Ok(path) => Ok(path),
            Err(_) => {
                self.prompt_custom_identity_path(Some(selected.as_str()))
                    .await
            }
        }
    }

    async fn prompt_custom_identity_path(&self, default: Option<&str>) -> AppResult<String> {
        let hint = default.unwrap_or("<none>");
        let help = format!("{hint} - SSH private key path used for authentication.");
        self.prompt_and_validate(
            "Identity path: ",
            &help,
            default,
            "Validating identity path...",
            |input| {
                if input.is_empty() {
                    return Err(AppError::invalid_argument(
                        "identity path is required in interactive mode",
                    ));
                }
                validate_identity_path(self.fs, input)?;
                Ok(input.to_string())
            },
            |path| format!("Identity path: {path}"),
        )
        .await
    }

    async fn validate_with_feedback<T, V, S>(
        &self,
        input: &str,
        validation_message: &str,
        mut validate: V,
        mut success_message: S,
    ) -> AppResult<T>
    where
        V: FnMut(&str) -> AppResult<T>,
        S: FnMut(&T) -> String,
    {
        let mut feedback = self.interaction.prompt_feedback().await?;
        feedback.start_validation(validation_message)?;
        match validate(input) {
            Ok(value) => {
                let message = success_message(&value);
                feedback.finish_success(&message)?;
                Ok(value)
            }
            Err(err) => {
                feedback.finish_failure(&validation_error_message(&err))?;
                Err(err)
            }
        }
    }

    async fn prompt_and_validate<T, V, S>(
        &self,
        prompt: &str,
        help: &str,
        default: Option<&str>,
        validation_message: &str,
        mut validate: V,
        mut success_message: S,
    ) -> AppResult<T>
    where
        V: FnMut(&str) -> AppResult<T>,
        S: FnMut(&T) -> String,
    {
        loop {
            let mut prompt_line = match default {
                Some(default) => {
                    self.interaction
                        .prompt_line_with_default_confirmable(prompt, help, default)
                        .await?
                }
                None => {
                    self.interaction
                        .prompt_line_confirmable(prompt, help)
                        .await?
                }
            };
            let input = prompt_line.input.trim().to_string();
            prompt_line.start_validation(validation_message)?;
            match validate(&input) {
                Ok(value) => {
                    let message = success_message(&value);
                    prompt_line.finish_success(&message)?;
                    return Ok(value);
                }
                Err(err) => {
                    let message = if err.message.trim().is_empty() {
                        "validation failed".to_string()
                    } else {
                        err.message.clone()
                    };
                    prompt_line.finish_failure(&message)?;
                }
            }
        }
    }
}

fn validation_error_message(err: &AppError) -> String {
    if err.message.trim().is_empty() {
        "validation failed".to_string()
    } else {
        err.message.clone()
    }
}

pub struct PathResolver<'a> {
    fs: &'a dyn FilesystemPort,
}

impl<'a> PathResolver<'a> {
    pub fn new(fs: &'a dyn FilesystemPort) -> Self {
        Self { fs }
    }

    pub fn resolve_local(&self, path: &str) -> AppResult<PathBuf> {
        let buf = PathBuf::from(path);
        self.fs.canonicalize(&buf).map_err(|err| {
            AppError::local_error(format!(
                "failed to resolve local path '{}': {}",
                buf.display(),
                err.message
            ))
        })
    }
}

pub struct SbatchSelector<'a> {
    fs: &'a dyn FilesystemPort,
    interaction: &'a dyn InteractionPort,
    ui_mode: UiMode,
}

impl<'a> SbatchSelector<'a> {
    pub fn new(
        fs: &'a dyn FilesystemPort,
        interaction: &'a dyn InteractionPort,
        ui_mode: UiMode,
    ) -> Self {
        Self {
            fs,
            interaction,
            ui_mode,
        }
    }

    pub async fn select(&self, local_path: &Path, explicit: Option<&str>) -> AppResult<String> {
        if let Some(value) = explicit {
            return Ok(value.to_string());
        }

        if !self.fs.is_dir(local_path)? {
            if !self.fs.is_file(local_path)? {
                return Err(AppError::invalid_argument(format!(
                    "local path '{}' does not exist",
                    local_path.display()
                )));
            }
            return Err(AppError::invalid_argument(format!(
                "local path '{}' must be a directory to auto-detect .sbatch scripts",
                local_path.display()
            )));
        }

        let scripts = collect_sbatch_scripts(self.fs, local_path)?;
        let relative_scripts: Vec<String> = scripts
            .iter()
            .map(|path| {
                let rel = path.strip_prefix(local_path).unwrap_or(path);
                rel.to_string_lossy().into_owned()
            })
            .collect();

        match relative_scripts.len() {
            0 => Err(AppError::invalid_argument(format!(
                "no .sbatch files found under '{}'; provide the script path explicitly",
                local_path.display()
            ))),
            1 => Ok(relative_scripts[0].clone()),
            _ => {
                if !self.ui_mode.is_interactive() {
                    let mut msg = format!(
                        "multiple .sbatch files found under '{}' while running in non-interactive mode; specify which one to use with --sbatchscript:\n",
                        local_path.display()
                    );
                    for script in &relative_scripts {
                        msg.push_str(&format!("  - {}\n", script));
                    }
                    return Err(AppError::invalid_argument(msg.trim_end()));
                }
                let selection = self
                    .interaction
                    .select_sbatch(&relative_scripts)
                    .await?
                    .ok_or_else(|| AppError::local_error("sbatch selection canceled"))?;
                Ok(selection)
            }
        }
    }

    pub async fn select_from_candidates(&self, candidates: &[String]) -> AppResult<String> {
        match candidates.len() {
            0 => Err(AppError::invalid_argument(
                "no .sbatch files found in blueprint metadata; provide the script path explicitly",
            )),
            1 => Ok(candidates[0].clone()),
            _ => {
                if !self.ui_mode.is_interactive() {
                    let mut msg = "multiple .sbatch files found while running in non-interactive mode; specify which one to use with --sbatchscript:\n".to_string();
                    for script in candidates {
                        msg.push_str(&format!("  - {script}\n"));
                    }
                    return Err(AppError::invalid_argument(msg.trim_end()));
                }
                let selection = self
                    .interaction
                    .select_sbatch(candidates)
                    .await?
                    .ok_or_else(|| AppError::local_error("sbatch selection canceled"))?;
                Ok(selection)
            }
        }
    }
}

/// locally validates if a suggested default base path breaks any rules
pub fn local_validate_default_base_path(base_path: &str) -> AppResult<()> {
    let trimmed = base_path.trim();
    if trimmed.is_empty() {
        return Err(AppError::invalid_argument(
            "default base path cannot be empty",
        ));
    }
    if trimmed == "~" || trimmed.starts_with("~/") {
        return Ok(());
    }
    if trimmed.starts_with('~') {
        return Err(AppError::invalid_argument(
            "default base path must be absolute or start with '~/' (use '~')",
        ));
    }
    if trimmed.starts_with('/') {
        return Ok(());
    }
    Err(AppError::invalid_argument(
        "default base path must be absolute or start with '~/' (use '~')",
    ))
}
/// Joins home directory with "runs" without fs access (for remote paths)
pub fn default_base_path_from_home(home_dir: &str) -> String {
    PathBuf::from(home_dir)
        .join("runs")
        .to_string_lossy()
        .into_owned()
}

#[derive(Debug)]
struct ParsedDestination {
    username: Option<String>,
    host: String,
    port: Option<u32>,
}

fn parse_destination(input: &str) -> AppResult<ParsedDestination> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err(AppError::invalid_argument("destination is required"));
    }

    let (username_part, host_part) = match trimmed.split_once('@') {
        Some((user, host)) => {
            if user.trim().is_empty() {
                return Err(AppError::invalid_argument(
                    "destination username is required",
                ));
            }
            (Some(user.trim().to_string()), host.trim())
        }
        None => {
            return Err(AppError::invalid_argument(
                "destination must be in user@host[:port] format",
            ));
        }
    };

    let host_part = host_part.trim();
    if host_part.is_empty() {
        return Err(AppError::invalid_argument("destination host is required"));
    }

    let (host, port) = if let Some(rest) = host_part.strip_prefix('[') {
        let Some((host, remainder)) = rest.split_once(']') else {
            return Err(AppError::invalid_argument(
                "destination IPv6 host must end with ']'",
            ));
        };
        let remainder = remainder.trim();
        if remainder.is_empty() {
            (host.to_string(), None)
        } else if let Some(port_str) = remainder.strip_prefix(':') {
            (host.to_string(), Some(parse_destination_port(port_str)?))
        } else {
            return Err(AppError::invalid_argument(
                "destination has unexpected trailing characters after ']'",
            ));
        }
    } else if host_part.matches(':').count() > 1 {
        if host_part.parse::<IpAddr>().is_err() {
            return Err(AppError::invalid_argument(
                "destination host must be IPv6 when using multiple ':' characters",
            ));
        }
        (host_part.to_string(), None)
    } else if let Some((host, port_str)) = host_part.rsplit_once(':') {
        if host.trim().is_empty() {
            return Err(AppError::invalid_argument("destination host is required"));
        }
        (
            host.trim().to_string(),
            Some(parse_destination_port(port_str)?),
        )
    } else {
        (host_part.to_string(), None)
    };

    Ok(ParsedDestination {
        username: username_part,
        host,
        port,
    })
}

fn parse_destination_port(value: &str) -> AppResult<u32> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(AppError::invalid_argument(
            "destination port is required after ':'",
        ));
    }
    match trimmed.parse::<u32>() {
        Ok(port) => Ok(port),
        Err(_) => Err(AppError::invalid_argument(
            "destination port must be a number",
        )),
    }
}

fn resolve_destination_port(port: Option<u32>) -> AppResult<u16> {
    let port = port.unwrap_or(DEFAULT_SSH_PORT);
    if port == 0 || port > u16::MAX as u32 {
        return Err(AppError::invalid_argument(
            "destination port must be between 1 and 65535",
        ));
    }
    Ok(port as u16)
}

fn default_name_for_host(hostname: Option<&str>, ip: Option<&str>) -> AppResult<String> {
    if let Some(host) = hostname {
        if host.parse::<IpAddr>().is_ok() {
            return Ok(generate_random_name());
        }
        return Ok(host.to_string());
    }
    if ip.is_some() {
        return Ok(generate_random_name());
    }
    Err(AppError::invalid_argument(
        "name default requires a hostname or ip",
    ))
}

fn generate_random_name() -> String {
    let mut rng = rand::rng();
    let adjective = ADJECTIVES.choose(&mut rng).copied().unwrap_or("curious");
    let scientist = SCIENTISTS.choose(&mut rng).copied().unwrap_or("einstein");
    format!("{adjective}_{scientist}")
}

fn validate_name(name: &str, existing_names: &HashSet<String>) -> AppResult<()> {
    if name.is_empty() {
        return Err(AppError::invalid_argument("name cannot be empty"));
    }
    if name.chars().any(char::is_whitespace) {
        return Err(AppError::invalid_argument("name cannot contain whitespace"));
    }
    if !name
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' || ch == '.')
    {
        return Err(AppError::invalid_argument(
            "name may only contain letters, numbers, '.', '-', and '_'",
        ));
    }
    if existing_names.contains(name) {
        return Err(AppError::invalid_argument("name already exists"));
    }
    Ok(())
}

fn normalize_option(value: Option<String>) -> Option<String> {
    value.and_then(|item| {
        let trimmed = item.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
}

fn validate_identity_path(fs: &dyn FilesystemPort, path: &str) -> AppResult<()> {
    let expanded = shellexpand::full(path)
        .map_err(|e| AppError::invalid_argument(format!("invalid identity path: {e}")))?
        .to_string();
    let path_buf = PathBuf::from(&expanded);
    if !fs.is_file(&path_buf)? {
        if fs.is_dir(&path_buf)? {
            return Err(AppError::invalid_argument("identity path is not a file"));
        }
        return Err(AppError::invalid_argument("identity path does not exist"));
    }
    let contents = fs
        .read_file(&path_buf)
        .map_err(|_| AppError::invalid_argument("identity path is not readable"))?;
    if contents.is_empty() {
        return Err(AppError::invalid_argument("identity path is empty"));
    }
    if !looks_like_identity_file(&contents) {
        return Err(AppError::invalid_argument(
            "identity path does not look like a private key",
        ));
    }
    Ok(())
}

fn looks_like_identity_file(contents: &[u8]) -> bool {
    let text = String::from_utf8_lossy(contents);
    const KEY_MARKERS: [&str; 7] = [
        "BEGIN OPENSSH PRIVATE KEY",
        "BEGIN RSA PRIVATE KEY",
        "BEGIN EC PRIVATE KEY",
        "BEGIN DSA PRIVATE KEY",
        "BEGIN ED25519 PRIVATE KEY",
        "BEGIN PRIVATE KEY",
        "BEGIN ENCRYPTED PRIVATE KEY",
    ];
    KEY_MARKERS.iter().any(|marker| text.contains(marker))
}

fn find_preferred_identity_path(fs: &dyn FilesystemPort) -> Option<String> {
    discover_identity_paths(fs).into_iter().next()
}

fn discover_identity_paths(fs: &dyn FilesystemPort) -> Vec<String> {
    // TODO: home dir discovery seems to be detail that should be hidden by the FilesystemPort
    let Some(home_dir) = dirs::home_dir() else {
        return Vec::new();
    };
    let ssh_dir = home_dir.join(".ssh");
    discover_identity_paths_in_dir(fs, &ssh_dir)
}

fn discover_identity_paths_in_dir(fs: &dyn FilesystemPort, ssh_dir: &Path) -> Vec<String> {
    let entries = match fs.read_dir(ssh_dir) {
        Ok(entries) => entries,
        Err(_) => return Vec::new(),
    };

    let mut ed25519 = Vec::new();
    let mut rsa = Vec::new();
    let mut other = Vec::new();

    for path in entries {
        if fs.is_dir(&path).unwrap_or(false) || !fs.is_file(&path).unwrap_or(false) {
            continue;
        }

        let file_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or_default();
        let normalized_name = file_name.to_ascii_lowercase();
        if normalized_name.ends_with(".pub") {
            continue;
        }
        if matches!(
            normalized_name.as_str(),
            "authorized_keys" | "known_hosts" | "known_hosts.old" | "config"
        ) {
            continue;
        }

        let path_str = path.to_string_lossy().into_owned();
        if validate_identity_path(fs, &path_str).is_err() {
            continue;
        }

        if normalized_name.contains("ed25519") {
            ed25519.push(path_str);
        } else if normalized_name.contains("rsa") {
            rsa.push(path_str);
        } else {
            other.push(path_str);
        }
    }

    ed25519.sort();
    rsa.sort();
    other.sort();

    let mut ordered = Vec::with_capacity(ed25519.len() + rsa.len() + other.len());
    ordered.extend(ed25519);
    ordered.extend(rsa);
    ordered.extend(other);
    ordered
}

fn build_identity_selector_options(identity_paths: Vec<String>) -> (Vec<String>, String) {
    let mut options = Vec::with_capacity(identity_paths.len() + 1);
    options.push(OTHER_IDENTITY_OPTION.to_string());
    options.extend(identity_paths);
    let default = options
        .get(1)
        .cloned()
        .unwrap_or_else(|| OTHER_IDENTITY_OPTION.to_string());
    (options, default)
}

fn collect_sbatch_scripts(fs: &dyn FilesystemPort, root: &Path) -> AppResult<Vec<PathBuf>> {
    let mut matches = Vec::new();
    let mut stack = vec![root.to_path_buf()];

    while let Some(dir) = stack.pop() {
        let entries = fs.read_dir(&dir).map_err(|err| {
            AppError::local_error(format!("failed to read {}: {}", dir.display(), err.message))
        })?;
        for path in entries {
            if fs.is_dir(&path)? {
                stack.push(path);
                continue;
            }
            if fs.is_file(&path)? && path.extension().map(|ext| ext == "sbatch").unwrap_or(false) {
                matches.push(path);
            }
        }
    }

    matches.sort();
    Ok(matches)
}

const ADJECTIVES: &[&str] = &[
    "bold", "brisk", "calm", "clever", "curious", "daring", "eager", "gentle", "grand", "happy",
    "jolly", "kind", "lively", "mighty", "noble", "proud", "quiet", "rapid", "sharp", "witty",
];

const SCIENTISTS: &[&str] = &[
    "bohr", "curie", "darwin", "einstein", "fermi", "feynman", "gauss", "galilei", "goodall",
    "hawking", "kepler", "lovelace", "mendel", "newton", "noether", "planck", "tesla", "turing",
    "weber", "wilson",
];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::errors::ErrorType;
    use std::fs::OpenOptions;
    use std::io::Write;
    use std::net::TcpListener;

    struct TestInteraction;

    #[tonic::async_trait]
    impl InteractionPort for TestInteraction {
        async fn confirm(&self, _prompt: &str, _help: &str) -> AppResult<bool> {
            Err(AppError::internal_error("unexpected confirm"))
        }

        async fn prompt_line(&self, _prompt: &str, _help: &str) -> AppResult<String> {
            Err(AppError::internal_error("unexpected prompt"))
        }

        async fn prompt_line_with_default(
            &self,
            _prompt: &str,
            _help: &str,
            _default: &str,
        ) -> AppResult<String> {
            Err(AppError::internal_error("unexpected prompt"))
        }

        async fn prompt_line_confirmable(
            &self,
            _prompt: &str,
            _help: &str,
        ) -> AppResult<crate::app::ports::PromptLine> {
            Err(AppError::internal_error("unexpected prompt"))
        }

        async fn prompt_line_with_default_confirmable(
            &self,
            _prompt: &str,
            _help: &str,
            _default: &str,
        ) -> AppResult<crate::app::ports::PromptLine> {
            Err(AppError::internal_error("unexpected prompt"))
        }

        async fn prompt_feedback(
            &self,
        ) -> AppResult<Box<dyn crate::app::ports::PromptFeedbackPort>> {
            Err(AppError::internal_error("unexpected prompt"))
        }

        async fn select_sbatch(&self, _options: &[String]) -> AppResult<Option<String>> {
            Err(AppError::internal_error("unexpected selection"))
        }

        async fn select_enum(
            &self,
            _name: &str,
            _options: &[String],
            _default: Option<&str>,
            _help: &str,
        ) -> AppResult<String> {
            Err(AppError::internal_error("unexpected selection"))
        }

        async fn prompt_mfa(&self, _mfa: &proto::MfaPrompt) -> AppResult<proto::MfaAnswer> {
            Err(AppError::internal_error("unexpected mfa"))
        }

        async fn prompt_mfa_transient(
            &self,
            _mfa: &proto::MfaPrompt,
        ) -> AppResult<(proto::MfaAnswer, usize)> {
            Err(AppError::internal_error("unexpected mfa"))
        }

        async fn clear_transient(&self, _lines: usize) -> AppResult<()> {
            Ok(())
        }
    }

    struct StdFilesystem;

    impl FilesystemPort for StdFilesystem {
        fn canonicalize(&self, path: &Path) -> AppResult<PathBuf> {
            std::fs::canonicalize(path).map_err(|e| AppError::local_error(e.to_string()))
        }

        fn current_dir(&self) -> AppResult<PathBuf> {
            std::env::current_dir().map_err(|e| AppError::local_error(e.to_string()))
        }

        fn read_dir(&self, path: &Path) -> AppResult<Vec<PathBuf>> {
            let mut entries = Vec::new();
            let iter = std::fs::read_dir(path).map_err(|e| AppError::local_error(e.to_string()))?;
            for entry in iter {
                let entry = entry.map_err(|e| AppError::local_error(e.to_string()))?;
                entries.push(entry.path());
            }
            Ok(entries)
        }

        fn is_file(&self, path: &Path) -> AppResult<bool> {
            Ok(path.is_file())
        }

        fn is_dir(&self, path: &Path) -> AppResult<bool> {
            Ok(path.is_dir())
        }

        fn read_file(&self, path: &Path) -> AppResult<Vec<u8>> {
            std::fs::read(path).map_err(|e| AppError::local_error(e.to_string()))
        }
    }

    struct StdNetwork;

    impl NetworkPort for StdNetwork {
        fn check_reachable(&self, _host: &str, _port: u16) -> AppResult<()> {
            Ok(())
        }
    }

    fn write_test_identity_file() -> std::io::Result<String> {
        let dir = std::env::temp_dir();
        let pid = std::process::id();
        let content =
            b"-----BEGIN OPENSSH PRIVATE KEY-----\nkey\n-----END OPENSSH PRIVATE KEY-----\n";
        for idx in 0..1000 {
            let path = dir.join(format!("orbit_test_identity_{pid}_{idx}.key"));
            let file = OpenOptions::new().write(true).create_new(true).open(&path);
            let mut file = match file {
                Ok(file) => file,
                Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(err) => return Err(err),
            };
            file.write_all(content)?;
            return Ok(path.to_string_lossy().into_owned());
        }
        Err(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            "failed to create temp identity file",
        ))
    }

    fn write_identity_file(dir: &Path, name: &str) {
        let content =
            b"-----BEGIN OPENSSH PRIVATE KEY-----\nkey\n-----END OPENSSH PRIVATE KEY-----\n";
        std::fs::write(dir.join(name), content).unwrap();
    }

    #[tokio::test]
    async fn resolve_add_cluster_non_interactive_defaults() {
        let listener = match TcpListener::bind("127.0.0.1:0") {
            Ok(listener) => listener,
            Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => {
                return;
            }
            Err(err) => panic!("failed to bind listener: {err}"),
        };
        let port = listener.local_addr().unwrap().port();
        let identity_path = match write_test_identity_file() {
            Ok(path) => path,
            Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => {
                return;
            }
            Err(err) => panic!("failed to create identity file: {err}"),
        };
        let args = AddClusterCommand {
            destination: Some(format!("alex@localhost:{port}")),
            name: None,
            identity_path: Some(identity_path.clone()),
            default_base_path: None,
            is_default: false,
        };

        let resolver = AddClusterResolver::new(
            &TestInteraction,
            &StdFilesystem,
            &StdNetwork,
            UiMode::NonInteractive,
        );
        let resolved = resolver.resolve(args, &HashSet::new()).await.unwrap();
        assert_eq!(resolved.port, port as u32);
        assert_eq!(resolved.identity_path, identity_path);
        assert_eq!(
            resolved.default_base_path.as_deref(),
            Some(DEFAULT_BASE_PATH)
        );
        assert_eq!(resolved.username, "alex");
        assert_eq!(resolved.name, "localhost");
    }

    #[tokio::test]
    async fn resolve_add_cluster_non_interactive_requires_destination() {
        let args = AddClusterCommand {
            destination: None,
            name: None,
            identity_path: Some("~/.ssh/id_rsa".into()),
            default_base_path: None,
            is_default: false,
        };

        let resolver = AddClusterResolver::new(
            &TestInteraction,
            &StdFilesystem,
            &StdNetwork,
            UiMode::NonInteractive,
        );
        let err = resolver.resolve(args, &HashSet::new()).await.unwrap_err();
        assert_eq!(err.kind, ErrorType::InvalidArgument);
        assert!(err.message.contains("destination"));
    }

    #[test]
    fn parse_destination_with_user_port() {
        let parsed = parse_destination("user@example.com:2222").unwrap();
        assert_eq!(parsed.username.as_deref(), Some("user"));
        assert_eq!(parsed.host, "example.com");
        assert_eq!(parsed.port, Some(2222));
    }

    #[test]
    fn parse_destination_requires_username() {
        let err = parse_destination("example.com:2222").unwrap_err();
        assert!(
            err.message
                .contains("destination must be in user@host[:port] format")
        );
    }

    #[tokio::test]
    async fn resolve_add_cluster_non_interactive_destination_reachable() {
        let listener = match TcpListener::bind("127.0.0.1:0") {
            Ok(listener) => listener,
            Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => {
                return;
            }
            Err(err) => panic!("failed to bind listener: {err}"),
        };
        let port = listener.local_addr().unwrap().port();
        let identity_path = match write_test_identity_file() {
            Ok(path) => path,
            Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => {
                return;
            }
            Err(err) => panic!("failed to create identity file: {err}"),
        };
        let args = AddClusterCommand {
            destination: Some(format!("alex@127.0.0.1:{port}")),
            name: Some("local".into()),
            identity_path: Some(identity_path),
            default_base_path: None,
            is_default: false,
        };

        let resolver = AddClusterResolver::new(
            &TestInteraction,
            &StdFilesystem,
            &StdNetwork,
            UiMode::NonInteractive,
        );
        let resolved = resolver.resolve(args, &HashSet::new()).await.unwrap();
        assert_eq!(resolved.username, "alex");
        assert_eq!(resolved.port, port as u32);
        assert_eq!(resolved.name, "local");
    }

    #[test]
    fn random_name_uses_known_words() {
        let name = generate_random_name();
        let mut parts = name.split('_');
        let adjective = parts.next().unwrap_or_default();
        let scientist = parts.next().unwrap_or_default();
        assert!(ADJECTIVES.contains(&adjective));
        assert!(SCIENTISTS.contains(&scientist));
    }

    #[test]
    fn validate_name_rejects_whitespace() {
        let err = validate_name("bad name", &HashSet::new()).unwrap_err();
        assert!(err.message.contains("whitespace"));
    }

    #[test]
    fn validate_name_accepts_hostname_style() {
        assert!(validate_name("gpu.cluster-01", &HashSet::new()).is_ok());
    }

    #[test]
    fn discover_identity_paths_orders_ed25519_then_rsa_then_other() {
        let root = temp_dir();
        write_identity_file(&root, "id_rsa");
        write_identity_file(&root, "id_ed25519");
        write_identity_file(&root, "zeta");
        write_identity_file(&root, "id_ed25519_work");
        write_identity_file(&root, "id_rsa_backup");
        std::fs::write(root.join("id_rsa.pub"), "ssh-rsa AAAA").unwrap();
        std::fs::write(root.join("known_hosts"), "known-host").unwrap();

        let discovered = discover_identity_paths_in_dir(&StdFilesystem, &root);
        let expected = vec![
            root.join("id_ed25519").to_string_lossy().to_string(),
            root.join("id_ed25519_work").to_string_lossy().to_string(),
            root.join("id_rsa").to_string_lossy().to_string(),
            root.join("id_rsa_backup").to_string_lossy().to_string(),
            root.join("zeta").to_string_lossy().to_string(),
        ];
        assert_eq!(discovered, expected);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn build_identity_selector_options_places_other_first_with_second_default() {
        let identities = vec![
            "/tmp/key-ed25519".to_string(),
            "/tmp/key-rsa".to_string(),
            "/tmp/key-other".to_string(),
        ];
        let (options, default) = build_identity_selector_options(identities);
        assert_eq!(options[0], OTHER_IDENTITY_OPTION);
        assert_eq!(options[1], "/tmp/key-ed25519");
        assert_eq!(default, "/tmp/key-ed25519");
    }

    #[test]
    fn build_identity_selector_options_defaults_to_other_when_no_keys() {
        let (options, default) = build_identity_selector_options(Vec::new());
        assert_eq!(options, vec![OTHER_IDENTITY_OPTION.to_string()]);
        assert_eq!(default, OTHER_IDENTITY_OPTION.to_string());
    }

    fn temp_dir() -> PathBuf {
        use std::sync::atomic::{AtomicUsize, Ordering};

        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        let base = std::env::temp_dir();
        let pid = std::process::id();

        loop {
            let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let dir = base.join(format!("cli_sbatch_{pid}_{nanos}_{counter}"));
            match std::fs::create_dir(&dir) {
                Ok(()) => return dir,
                Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(err) => panic!("failed to create temp dir: {err}"),
            }
        }
    }

    #[tokio::test]
    async fn sbatch_selector_finds_nested_files() {
        let root = temp_dir();
        let nested = root.join("nested");
        std::fs::create_dir_all(&nested).unwrap();
        let one = root.join("a.sbatch");
        let two = nested.join("b.sbatch");
        std::fs::write(&one, "echo one").unwrap();
        std::fs::write(&two, "echo two").unwrap();

        let selector =
            SbatchSelector::new(&StdFilesystem, &TestInteraction, UiMode::NonInteractive);
        let err = selector.select(&root, None).await.unwrap_err();
        assert!(err.message.contains("multiple .sbatch files found"));
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn sbatch_selector_accepts_explicit_path() {
        let root = temp_dir();
        let selector =
            SbatchSelector::new(&StdFilesystem, &TestInteraction, UiMode::NonInteractive);
        let script = selector.select(&root, Some("job.sbatch")).await.unwrap();
        assert_eq!(script, "job.sbatch");
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn sbatch_selector_returns_single_script() {
        let root = temp_dir();
        let script = root.join("only.sbatch");
        std::fs::write(&script, "echo one").unwrap();

        let selector =
            SbatchSelector::new(&StdFilesystem, &TestInteraction, UiMode::NonInteractive);
        let selected = selector.select(&root, None).await.unwrap();
        assert_eq!(selected, "only.sbatch");
        let _ = std::fs::remove_dir_all(&root);
    }
}
