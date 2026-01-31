// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

use std::path::{Path, PathBuf};

use crate::app::errors::{AppError, AppResult};
use crate::app::ports::FilesystemPort;

pub struct StdFilesystem;

impl FilesystemPort for StdFilesystem {
    fn canonicalize(&self, path: &Path) -> AppResult<PathBuf> {
        std::fs::canonicalize(path).map_err(|err| AppError::local_error(err.to_string()))
    }

    fn current_dir(&self) -> AppResult<PathBuf> {
        std::env::current_dir().map_err(|err| AppError::local_error(err.to_string()))
    }

    fn read_dir(&self, path: &Path) -> AppResult<Vec<PathBuf>> {
        let mut entries = Vec::new();
        let iter = std::fs::read_dir(path).map_err(|err| AppError::local_error(err.to_string()))?;
        for entry in iter {
            let entry = entry.map_err(|err| AppError::local_error(err.to_string()))?;
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
        std::fs::read(path).map_err(|err| AppError::local_error(err.to_string()))
    }
}
