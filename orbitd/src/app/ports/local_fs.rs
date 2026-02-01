// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

use std::path::{Path, PathBuf};

use async_trait::async_trait;

use crate::app::errors::AppResult;

#[async_trait]
/// Local filesystem boundary for the core.
/// Provides current dir resolution and file reads with consistent errors.
pub trait LocalFilesystemPort: Send + Sync {
    async fn current_dir(&self) -> AppResult<PathBuf>;
    async fn read_to_string(&self, path: &Path) -> AppResult<String>;
}
