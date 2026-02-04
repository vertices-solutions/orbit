// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

use std::fs;
use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::app::errors::{AppError, AppResult};
use crate::app::ports::ConfigPort;

const APP_DIR_NAME: &str = "orbit";
const CONFIG_FILE_NAME: &str = "orbit.toml";
const CONFIG_ENV_VAR: &str = "ORBIT_CONFIG_PATH";
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
    let (config_path, required) = resolve_config_path(config_path_override)?;

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

fn resolve_config_path(config_path_override: Option<PathBuf>) -> AppResult<(Option<PathBuf>, bool)> {
    if let Some(path) = config_path_override {
        return Ok((Some(expand_path(path)), true));
    }
    if let Some(path) = config_path_from_env()? {
        return Ok((Some(expand_path(path)), true));
    }
    Ok((default_config_path().ok(), false))
}

fn config_path_from_env() -> AppResult<Option<PathBuf>> {
    match std::env::var_os(CONFIG_ENV_VAR) {
        Some(value) => {
            if value.is_empty() {
                return Err(AppError::invalid_argument(format!(
                    "{CONFIG_ENV_VAR} is set but empty"
                )));
            }
            Ok(Some(PathBuf::from(value)))
        }
        None => Ok(None),
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsString;
    use std::fs;
    use std::path::Path;
    use std::sync::Mutex;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    struct EnvVarGuard {
        key: &'static str,
        prev: Option<OsString>,
    }

    impl EnvVarGuard {
        fn set(key: &'static str, value: &str) -> Self {
            let prev = std::env::var_os(key);
            // SAFETY: tests serialize env mutations with ENV_LOCK.
            unsafe {
                std::env::set_var(key, value);
            }
            Self { key, prev }
        }

        fn clear(key: &'static str) -> Self {
            let prev = std::env::var_os(key);
            // SAFETY: tests serialize env mutations with ENV_LOCK.
            unsafe {
                std::env::remove_var(key);
            }
            Self { key, prev }
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            match &self.prev {
                Some(value) => {
                    // SAFETY: tests serialize env mutations with ENV_LOCK.
                    unsafe {
                        std::env::set_var(self.key, value);
                    }
                }
                None => {
                    // SAFETY: tests serialize env mutations with ENV_LOCK.
                    unsafe {
                        std::env::remove_var(self.key);
                    }
                }
            }
        }
    }

    struct TempDirGuard {
        path: PathBuf,
    }

    impl TempDirGuard {
        fn new() -> Self {
            let mut path = std::env::temp_dir();
            path.push(format!("orbit-config-test-{}", rand::random::<u64>()));
            fs::create_dir_all(&path).unwrap();
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TempDirGuard {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    #[test]
    fn env_config_path_used_when_no_override() {
        let _lock = ENV_LOCK.lock().unwrap();
        let _env = EnvVarGuard::clear(CONFIG_ENV_VAR);
        let dir = TempDirGuard::new();
        let config_path = dir.path().join("orbit.toml");
        fs::write(&config_path, "port = 40001\n").unwrap();
        let _env = EnvVarGuard::set(CONFIG_ENV_VAR, config_path.to_str().unwrap());

        let config = load_config(None).unwrap();
        assert_eq!(config.port, 40001);
    }

    #[test]
    fn cli_config_path_takes_precedence_over_env() {
        let _lock = ENV_LOCK.lock().unwrap();
        let _env = EnvVarGuard::clear(CONFIG_ENV_VAR);
        let dir = TempDirGuard::new();
        let env_path = dir.path().join("env.toml");
        let cli_path = dir.path().join("cli.toml");
        fs::write(&env_path, "port = 40001\n").unwrap();
        fs::write(&cli_path, "port = 40002\n").unwrap();
        let _env = EnvVarGuard::set(CONFIG_ENV_VAR, env_path.to_str().unwrap());

        let config = load_config(Some(cli_path)).unwrap();
        assert_eq!(config.port, 40002);
    }
}
