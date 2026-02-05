// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

use std::path::{Path, PathBuf};

use async_trait::async_trait;

use crate::app::errors::{AppError, AppErrorKind, AppResult, codes};
use crate::app::ports::LocalFilesystemPort;

#[derive(Clone, Default)]
pub struct LocalFilesystem;

impl LocalFilesystem {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl LocalFilesystemPort for LocalFilesystem {
    #[tracing::instrument(name = "fs", level = "debug", skip(self), fields(op = "current_dir"))]
    async fn current_dir(&self) -> AppResult<PathBuf> {
        std::env::current_dir().map_err(|err| {
            AppError::with_message(
                AppErrorKind::Internal,
                codes::LOCAL_ERROR,
                format!("failed to resolve current directory: {err}"),
            )
        })
    }

    #[tracing::instrument(name = "fs", level = "debug", skip(self, path), fields(op = "read_to_string", path = %path.display()))]
    async fn read_to_string(&self, path: &Path) -> AppResult<String> {
        std::fs::read_to_string(path).map_err(|err| {
            AppError::with_message(
                AppErrorKind::Internal,
                codes::LOCAL_ERROR,
                format!("failed to read {}: {err}", path.display()),
            )
        })
    }
}
