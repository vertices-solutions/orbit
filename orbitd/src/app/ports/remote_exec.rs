// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

use async_trait::async_trait;

use crate::app::errors::AppResult;
use crate::app::ports::{MfaPort, RunStreamOutputPort, StreamOutputPort};
use crate::app::types::SshConfig;

#[derive(Debug, Clone)]
pub struct ExecCapture {
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub exit_code: i32,
}

#[async_trait]
/// Remote command execution and SSH session lifecycle boundary.
/// Supports captured or streamed exec plus connection management.
pub trait RemoteExecPort: Send + Sync {
    async fn exec_capture(&self, config: &SshConfig, command: &str) -> AppResult<ExecCapture>;

    async fn exec_stream(
        &self,
        config: &SshConfig,
        command: &str,
        stream: &dyn StreamOutputPort,
        mfa: &mut dyn MfaPort,
    ) -> AppResult<()>;

    async fn ensure_connected(
        &self,
        config: &SshConfig,
        stream: &dyn StreamOutputPort,
        mfa: &mut dyn MfaPort,
    ) -> AppResult<()>;

    async fn ensure_connected_submit(
        &self,
        config: &SshConfig,
        stream: &dyn RunStreamOutputPort,
        mfa: &mut dyn MfaPort,
    ) -> AppResult<()>;

    async fn needs_connect(&self, config: &SshConfig) -> AppResult<bool>;

    async fn directory_exists(&self, config: &SshConfig, remote_dir: &str) -> AppResult<bool>;

    async fn is_connected(&self, session_name: &str) -> AppResult<bool>;

    async fn remove_session(&self, session_name: &str) -> AppResult<bool>;
}
