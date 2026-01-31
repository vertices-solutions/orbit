// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

use std::path::PathBuf;

use crate::app::errors::{AppError, AppResult};
use crate::app::ports::ConfigPort;

pub struct ConfigAdapter;

impl ConfigPort for ConfigAdapter {
    fn daemon_endpoint(&self, config_path: Option<PathBuf>) -> AppResult<String> {
        crate::config::daemon_endpoint(config_path)
            .map_err(|err| AppError::local_error(err.to_string()))
    }
}
