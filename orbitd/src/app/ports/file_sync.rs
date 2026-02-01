// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

use async_trait::async_trait;
use std::path::Path;

use crate::app::errors::AppResult;
use crate::app::ports::{MfaPort, SubmitStreamOutputPort};
use crate::app::types::{SshConfig, SyncOptions};

#[async_trait]
pub trait FileSyncPort: Send + Sync {
    async fn sync_dir(
        &self,
        config: &SshConfig,
        local_dir: &Path,
        remote_dir: &str,
        options: SyncOptions,
        stream: &dyn SubmitStreamOutputPort,
        mfa: &mut dyn MfaPort,
    ) -> AppResult<()>;

    async fn retrieve_path(
        &self,
        config: &SshConfig,
        remote_path: &str,
        local_path: &Path,
        overwrite: bool,
    ) -> AppResult<()>;
}
