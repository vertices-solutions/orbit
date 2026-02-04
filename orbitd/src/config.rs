// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

use anyhow::{Context, Result};
use serde::Deserialize;
use std::{
    fs,
    path::{Path, PathBuf},
};

const APP_DIR_NAME: &str = "orbit";
const CONFIG_FILE_NAME: &str = "orbit.toml";
const CONFIG_ENV_VAR: &str = "ORBIT_CONFIG_PATH";
const DATABASE_FILE_NAME: &str = "orbit.sqlite";
const DEFAULT_JOB_CHECK_INTERVAL_SECS: u64 = 5;
const DEFAULT_PORT: u16 = 50056;

#[derive(Debug, Default, Deserialize)]
struct FileConfig {
    database_path: Option<String>,
    job_check_interval_secs: Option<u64>,
    port: Option<u16>,
    verbose: Option<bool>,
}

#[derive(Debug)]
pub struct Config {
    pub database_path: PathBuf,
    pub job_check_interval_secs: u64,
    pub port: u16,
    pub verbose: bool,
    #[allow(dead_code)]
    pub config_path: Option<PathBuf>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigSource {
    Override,
    Env,
    ConfigFile,
    Default,
}

impl ConfigSource {
    pub fn as_str(self) -> &'static str {
        match self {
            ConfigSource::Override => "override",
            ConfigSource::Env => "env",
            ConfigSource::ConfigFile => "config",
            ConfigSource::Default => "default",
        }
    }
}

#[derive(Debug)]
pub struct ConfigValue<T> {
    pub value: T,
    pub source: ConfigSource,
}

#[derive(Debug)]
pub struct ConfigReport {
    pub config_path: Option<PathBuf>,
    pub config_path_source: Option<ConfigSource>,
    pub config_file_present: bool,
    pub database_path: ConfigValue<PathBuf>,
    pub job_check_interval_secs: ConfigValue<u64>,
    pub port: ConfigValue<u16>,
    pub verbose: ConfigValue<bool>,
}

#[derive(Debug)]
pub struct LoadResult {
    pub config: Config,
    pub report: ConfigReport,
}

#[derive(Debug, Default)]
pub struct Overrides {
    pub database_path: Option<PathBuf>,
    pub job_check_interval_secs: Option<u64>,
    pub port: Option<u16>,
    pub verbose: Option<bool>,
}

#[allow(dead_code)]
pub fn load(config_path_override: Option<PathBuf>, overrides: Overrides) -> Result<Config> {
    Ok(load_with_report(config_path_override, overrides)?.config)
}

pub fn load_with_report(
    config_path_override: Option<PathBuf>,
    overrides: Overrides,
) -> Result<LoadResult> {
    let (config_path, config_path_source, required) = match config_path_override {
        Some(path) => (Some(expand_path(path)), Some(ConfigSource::Override), true),
        None => match config_path_from_env()? {
            Some(path) => (Some(expand_path(path)), Some(ConfigSource::Env), true),
            None => match default_config_path().ok() {
                Some(path) => (Some(path), Some(ConfigSource::Default), false),
                None => (None, None, false),
            },
        },
    };
    let config_file_present = config_path
        .as_deref()
        .map(|path| path.exists())
        .unwrap_or(false);

    let file_config = match config_path.as_deref() {
        Some(path) => read_config_file(path, required)?,
        None => FileConfig::default(),
    };

    let (database_path, database_source) = match overrides.database_path {
        Some(path) => (expand_path(path), ConfigSource::Override),
        None => match file_config.database_path {
            Some(raw) => (
                resolve_path(
                    &raw,
                    config_path.as_deref().and_then(|path| path.parent()),
                ),
                ConfigSource::ConfigFile,
            ),
            None => (
                default_database_path().with_context(|| {
                    "failed to resolve default database path; specify --database-path or set database_path in the config file"
                })?,
                ConfigSource::Default,
            ),
        },
    };

    let (port, port_source) = match overrides.port {
        Some(port) => (port, ConfigSource::Override),
        None => match file_config.port {
            Some(port) => (port, ConfigSource::ConfigFile),
            None => (DEFAULT_PORT, ConfigSource::Default),
        },
    };
    if port == 0 {
        anyhow::bail!("port must be between 1 and 65535");
    }
    let (verbose, verbose_source) = match overrides.verbose {
        Some(verbose) => (verbose, ConfigSource::Override),
        None => match file_config.verbose {
            Some(verbose) => (verbose, ConfigSource::ConfigFile),
            None => (false, ConfigSource::Default),
        },
    };

    let (job_check_interval_secs, job_check_interval_source) =
        match overrides.job_check_interval_secs {
            Some(secs) => (secs, ConfigSource::Override),
            None => match file_config.job_check_interval_secs {
                Some(secs) => (secs, ConfigSource::ConfigFile),
                None => (DEFAULT_JOB_CHECK_INTERVAL_SECS, ConfigSource::Default),
            },
        };

    let config = Config {
        database_path,
        job_check_interval_secs,
        port,
        verbose,
        config_path: config_path.clone(),
    };

    let report = ConfigReport {
        config_path,
        config_path_source,
        config_file_present,
        database_path: ConfigValue {
            value: config.database_path.clone(),
            source: database_source,
        },
        job_check_interval_secs: ConfigValue {
            value: config.job_check_interval_secs,
            source: job_check_interval_source,
        },
        port: ConfigValue {
            value: config.port,
            source: port_source,
        },
        verbose: ConfigValue {
            value: config.verbose,
            source: verbose_source,
        },
    };

    Ok(LoadResult { config, report })
}

pub fn ensure_database_dir(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create database directory {}", parent.display()))?;
    }
    Ok(())
}

fn read_config_file(path: &Path, required: bool) -> Result<FileConfig> {
    if !path.exists() {
        if required {
            anyhow::bail!("config file not found at {}", path.display());
        }
        return Ok(FileConfig::default());
    }

    let contents = fs::read_to_string(path)
        .with_context(|| format!("failed to read config file {}", path.display()))?;
    toml::from_str(&contents)
        .with_context(|| format!("failed to parse config file {}", path.display()))
}

fn resolve_path(raw: &str, base_dir: Option<&Path>) -> PathBuf {
    let expanded = shellexpand::tilde(raw);
    let path = PathBuf::from(expanded.as_ref());
    if path.is_absolute() {
        return path;
    }
    match base_dir {
        Some(dir) => dir.join(path),
        None => path,
    }
}

fn expand_path(path: PathBuf) -> PathBuf {
    let path_string = path.to_string_lossy().to_string();
    let expanded = shellexpand::tilde(&path_string);
    PathBuf::from(expanded.as_ref())
}

fn config_path_from_env() -> Result<Option<PathBuf>> {
    match std::env::var_os(CONFIG_ENV_VAR) {
        Some(value) => {
            if value.is_empty() {
                anyhow::bail!("{CONFIG_ENV_VAR} is set but empty");
            }
            Ok(Some(PathBuf::from(value)))
        }
        None => Ok(None),
    }
}

fn default_config_path() -> Result<PathBuf> {
    Ok(default_config_dir()?.join(CONFIG_FILE_NAME))
}

fn default_database_path() -> Result<PathBuf> {
    Ok(default_data_dir()?.join(DATABASE_FILE_NAME))
}

fn default_config_dir() -> Result<PathBuf> {
    let base = dirs::config_dir().context("failed to resolve config directory")?;
    Ok(base.join(APP_DIR_NAME))
}

fn default_data_dir() -> Result<PathBuf> {
    let base = dirs::data_dir().context("failed to resolve data directory")?;
    Ok(base.join(APP_DIR_NAME))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::sync::Mutex;
    use std::ffi::OsString;
    use tempfile::TempDir;

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

    #[test]
    fn missing_optional_config_file_is_ok() {
        let dir = TempDir::new().unwrap();
        let config_path = dir.path().join("missing.toml");
        let cfg = read_config_file(&config_path, false).unwrap();
        assert!(cfg.database_path.is_none());
        assert!(cfg.job_check_interval_secs.is_none());
    }

    #[test]
    fn missing_required_config_file_errors() {
        let dir = TempDir::new().unwrap();
        let config_path = dir.path().join("missing.toml");
        let err = read_config_file(&config_path, true).unwrap_err();
        assert!(err.to_string().contains("config file not found"));
    }

    #[test]
    fn resolves_relative_database_path_from_config_dir() {
        let dir = TempDir::new().unwrap();
        let config_dir = dir.path().join("config");
        fs::create_dir_all(&config_dir).unwrap();
        let config_path = config_dir.join("orbit.toml");
        fs::write(
            &config_path,
            "database_path = \"db/orbit.sqlite\"\njob_check_interval_secs = 9\n",
        )
        .unwrap();

        let config_path = config_path;
        let config = load(Some(config_path.clone()), Overrides::default()).unwrap();
        assert_eq!(
            config.database_path,
            config_dir.join("db").join("orbit.sqlite")
        );
        assert_eq!(config.job_check_interval_secs, 9);
        assert_eq!(config.port, DEFAULT_PORT);
        assert_eq!(config.config_path, Some(config_path));
    }

    #[test]
    fn cli_overrides_take_precedence_over_file_config() {
        let dir = TempDir::new().unwrap();
        let config_dir = dir.path().join("config");
        fs::create_dir_all(&config_dir).unwrap();
        let config_path = config_dir.join("orbit.toml");
        fs::write(
            &config_path,
            "database_path = \"db/from_config.sqlite\"\njob_check_interval_secs = 9\n",
        )
        .unwrap();

        let config = load(
            Some(config_path),
            Overrides {
                database_path: Some(PathBuf::from("from_flag.sqlite")),
                job_check_interval_secs: Some(2),
                port: None,
                verbose: None,
            },
        )
        .unwrap();

        assert_eq!(config.database_path, PathBuf::from("from_flag.sqlite"));
        assert_eq!(config.job_check_interval_secs, 2);
    }

    #[test]
    fn overrides_apply_per_field() {
        let dir = TempDir::new().unwrap();
        let config_dir = dir.path().join("config");
        fs::create_dir_all(&config_dir).unwrap();
        let config_path = config_dir.join("orbit.toml");
        fs::write(
            &config_path,
            "database_path = \"db/from_config.sqlite\"\njob_check_interval_secs = 9\n",
        )
        .unwrap();

        let config = load(
            Some(config_path),
            Overrides {
                database_path: None,
                job_check_interval_secs: Some(2),
                port: None,
                verbose: None,
            },
        )
        .unwrap();

        assert_eq!(
            config.database_path,
            config_dir.join("db").join("from_config.sqlite")
        );
        assert_eq!(config.job_check_interval_secs, 2);
    }

    #[test]
    fn uses_default_job_check_interval_when_missing() {
        let dir = TempDir::new().unwrap();
        let config_dir = dir.path().join("config");
        fs::create_dir_all(&config_dir).unwrap();
        let config_path = config_dir.join("orbit.toml");
        fs::write(&config_path, "database_path = \"db/orbit.sqlite\"\n").unwrap();

        let config = load(Some(config_path), Overrides::default()).unwrap();
        assert_eq!(
            config.job_check_interval_secs,
            DEFAULT_JOB_CHECK_INTERVAL_SECS
        );
        assert_eq!(config.port, DEFAULT_PORT);
    }

    #[test]
    fn reads_verbose_from_config() {
        let dir = TempDir::new().unwrap();
        let config_dir = dir.path().join("config");
        fs::create_dir_all(&config_dir).unwrap();
        let config_path = config_dir.join("orbit.toml");
        fs::write(
            &config_path,
            "database_path = \"db/orbit.sqlite\"\nverbose = true\n",
        )
        .unwrap();

        let config = load(Some(config_path), Overrides::default()).unwrap();
        assert!(config.verbose);
    }

    #[test]
    fn reads_port_from_config() {
        let dir = TempDir::new().unwrap();
        let config_dir = dir.path().join("config");
        fs::create_dir_all(&config_dir).unwrap();
        let config_path = config_dir.join("orbit.toml");
        fs::write(
            &config_path,
            "database_path = \"db/orbit.sqlite\"\nport = 40001\n",
        )
        .unwrap();

        let config = load(Some(config_path), Overrides::default()).unwrap();
        assert_eq!(config.port, 40001);
    }

    #[test]
    fn port_override_takes_precedence() {
        let dir = TempDir::new().unwrap();
        let config_dir = dir.path().join("config");
        fs::create_dir_all(&config_dir).unwrap();
        let config_path = config_dir.join("orbit.toml");
        fs::write(
            &config_path,
            "database_path = \"db/orbit.sqlite\"\nport = 40001\n",
        )
        .unwrap();

        let config = load(
            Some(config_path),
            Overrides {
                database_path: None,
                job_check_interval_secs: None,
                port: Some(40002),
                verbose: None,
            },
        )
        .unwrap();

        assert_eq!(config.port, 40002);
    }

    #[test]
    fn verbose_override_takes_precedence() {
        let dir = TempDir::new().unwrap();
        let config_dir = dir.path().join("config");
        fs::create_dir_all(&config_dir).unwrap();
        let config_path = config_dir.join("orbit.toml");
        fs::write(
            &config_path,
            "database_path = \"db/orbit.sqlite\"\nverbose = false\n",
        )
        .unwrap();

        let config = load(
            Some(config_path),
            Overrides {
                database_path: None,
                job_check_interval_secs: None,
                port: None,
                verbose: Some(true),
            },
        )
        .unwrap();

        assert!(config.verbose);
    }

    #[test]
    fn ensure_database_dir_creates_parent_directory() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("nested").join("orbit.sqlite");
        ensure_database_dir(&db_path).unwrap();
        assert!(dir.path().join("nested").is_dir());
    }

    #[test]
    fn ensure_database_dir_no_parent_does_not_error() {
        ensure_database_dir(Path::new("orbit.sqlite")).unwrap();
    }

    #[test]
    fn env_config_path_used_when_no_override() {
        let _lock = ENV_LOCK.lock().unwrap();
        let _env = EnvVarGuard::clear(CONFIG_ENV_VAR);
        let dir = TempDir::new().unwrap();
        let config_path = dir.path().join("orbit.toml");
        fs::write(
            &config_path,
            "database_path = \"db/orbit.sqlite\"\nport = 40001\n",
        )
        .unwrap();
        let _env = EnvVarGuard::set(CONFIG_ENV_VAR, config_path.to_str().unwrap());

        let LoadResult { config, report } =
            load_with_report(None, Overrides::default()).unwrap();
        assert_eq!(config.port, 40001);
        assert_eq!(config.config_path, Some(config_path));
        assert_eq!(report.config_path_source, Some(ConfigSource::Env));
    }

    #[test]
    fn cli_config_path_takes_precedence_over_env() {
        let _lock = ENV_LOCK.lock().unwrap();
        let _env = EnvVarGuard::clear(CONFIG_ENV_VAR);
        let dir = TempDir::new().unwrap();
        let env_path = dir.path().join("env.toml");
        let cli_path = dir.path().join("cli.toml");
        fs::write(
            &env_path,
            "database_path = \"db/orbit.sqlite\"\nport = 40001\n",
        )
        .unwrap();
        fs::write(
            &cli_path,
            "database_path = \"db/orbit.sqlite\"\nport = 40002\n",
        )
        .unwrap();
        let _env = EnvVarGuard::set(CONFIG_ENV_VAR, env_path.to_str().unwrap());

        let LoadResult { config, report } =
            load_with_report(Some(cli_path.clone()), Overrides::default()).unwrap();
        assert_eq!(config.port, 40002);
        assert_eq!(config.config_path, Some(cli_path));
        assert_eq!(report.config_path_source, Some(ConfigSource::Override));
    }
}
