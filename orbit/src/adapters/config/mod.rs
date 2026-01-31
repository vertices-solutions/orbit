// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

use std::path::{Path, PathBuf};
use std::fs;

use serde::Deserialize;

use crate::app::errors::{AppError, AppResult};
use crate::app::ports::ConfigPort;

const APP_DIR_NAME: &str = "orbit";
const CONFIG_FILE_NAME: &str = "orbit.toml";
const DEFAULT_PORT: u16 = 50056;
const DEFAULT_HOST: &str = "127.0.0.1";

#[derive(Debug, Default, Deserialize)]
struct FileConfig {
    port: Option<u16>,
}

#[derive(Debug)]
struct Config {
    port: u16,
}

pub struct ConfigAdapter;

impl ConfigPort for ConfigAdapter {
    fn daemon_endpoint(&self, config_path: Option<PathBuf>) -> AppResult<String> {
        let config = load_config(config_path)?;
        Ok(format!("http://{DEFAULT_HOST}:{}", config.port))
    }
}

fn load_config(config_path_override: Option<PathBuf>) -> AppResult<Config> {
    let required = config_path_override.is_some();
    let config_path = resolve_config_path(config_path_override)?;

    let file_config = match config_path.as_deref() {
        Some(path) => read_config_file(path, required)?,
        None => FileConfig::default(),
    };

    let port = file_config.port.unwrap_or(DEFAULT_PORT);
    if port == 0 {
        return Err(AppError::invalid_argument(
            "port must be between 1 and 65535",
        ));
    }

    Ok(Config { port })
}

fn resolve_config_path(config_path_override: Option<PathBuf>) -> AppResult<Option<PathBuf>> {
    Ok(match config_path_override {
        Some(path) => Some(expand_path(path)),
        None => default_config_path().ok(),
    })
}

fn read_config_file(path: &Path, required: bool) -> AppResult<FileConfig> {
    if !path.exists() {
        if required {
            return Err(AppError::local_error(format!(
                "config file not found at {}",
                path.display()
            )));
        }
        return Ok(FileConfig::default());
    }

    let contents = fs::read_to_string(path).map_err(|err| {
        AppError::local_error(format!(
            "failed to read config file {}: {}",
            path.display(),
            err
        ))
    })?;
    toml::from_str(&contents).map_err(|err| {
        AppError::local_error(format!(
            "failed to parse config file {}: {}",
            path.display(),
            err
        ))
    })
}

fn expand_path(path: PathBuf) -> PathBuf {
    let path_string = path.to_string_lossy().to_string();
    let expanded = shellexpand::tilde(&path_string);
    PathBuf::from(expanded.as_ref())
}

fn default_config_path() -> AppResult<PathBuf> {
    Ok(default_config_dir()?.join(CONFIG_FILE_NAME))
}

fn default_config_dir() -> AppResult<PathBuf> {
    let base = dirs::config_dir()
        .ok_or_else(|| AppError::local_error("failed to resolve config directory"))?;
    Ok(base.join(APP_DIR_NAME))
}
