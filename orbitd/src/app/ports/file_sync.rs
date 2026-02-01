// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

use async_trait::async_trait;
use std::path::Path;

use crate::app::errors::AppResult;
use crate::app::ports::{MfaPort, SubmitStreamOutputPort};
use crate::app::types::{SshConfig, SyncOptions};

#[async_trait]
/// File transfer/sync boundary between local and remote paths.
/// Supports submit-time local-to-remote sync with streaming and remote-to-local retrieval.
/// Send+Sync because FileSyncPort is held inside UseCases as Arc<dyn FileSyncPort>, and UseCases is
/// cloned and used concurrently by tonic handlers (which spawn tasks on a multithreaded Tokio runtime).
/// Under normal conditions, there should be only one adapter implementing it (i.e., SshAdapter)
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
