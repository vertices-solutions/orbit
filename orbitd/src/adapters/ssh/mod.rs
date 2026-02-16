// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use proto::{MfaAnswer, RunStreamEvent, StreamEvent};
use tokio::sync::mpsc;

use crate::app::errors::{AppError, AppErrorKind, AppResult, codes};
use crate::app::ports::{
    FileSyncPort, MfaPort, RemoteExecPort, RunStreamOutputPort, StreamOutputPort,
};
use crate::app::types::{
    SshConfig, SyncFilterAction as AppSyncFilterAction, SyncFilterRule as AppSyncFilterRule,
    SyncOptions as AppSyncOptions,
};

mod error;
mod session;
mod sync;
mod sync_plan;
mod utils;

pub mod session_cache;

use session_cache::{DefaultSessionFactory, SessionCache};

pub use error::AuthenticationFailure;
pub use session::{SessionManager, SshParams};
pub use sync::{SyncFilterAction, SyncFilterRule, SyncOptions};
pub use utils::receiver_to_stream;

#[derive(Clone)]
pub struct SshAdapter {
    sessions: Arc<SessionCache>,
}

impl SshAdapter {
    pub fn new(sessions: Arc<SessionCache>) -> Self {
        Self { sessions }
    }

    pub fn with_defaults() -> Self {
        let factory = Arc::new(DefaultSessionFactory);
        let sessions = Arc::new(SessionCache::new(factory));
        Self::new(sessions)
    }
}

fn ssh_error_code(err: &anyhow::Error) -> &'static str {
    if err.chain().any(|cause| cause.is::<AuthenticationFailure>()) {
        codes::AUTHENTICATION_FAILURE
    } else {
        codes::CONNECTION_FAILURE
    }
}

fn map_exec_error(err: anyhow::Error) -> AppError {
    AppError::with_message(
        AppErrorKind::Internal,
        codes::REMOTE_ERROR,
        format!("ssh exec failed: {err}"),
    )
}

fn map_connect_error(err: anyhow::Error) -> AppError {
    AppError::with_message(
        AppErrorKind::Aborted,
        ssh_error_code(&err),
        format!("ssh connect failed: {err}"),
    )
}

fn map_directory_error(err: String) -> AppError {
    AppError::with_message(
        AppErrorKind::Internal,
        codes::REMOTE_ERROR,
        format!("ssh directory check failed: {err}"),
    )
}

fn is_sftp_missing_path(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| {
        let Some(sftp_error) = cause.downcast_ref::<russh_sftp::client::error::Error>() else {
            return false;
        };
        matches!(
            sftp_error,
            russh_sftp::client::error::Error::Status(status)
                if status.status_code == russh_sftp::protocol::StatusCode::NoSuchFile
        )
    })
}

fn is_local_path_conflict(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| {
        let Some(io_err) = cause.downcast_ref::<std::io::Error>() else {
            return false;
        };
        io_err.kind() == std::io::ErrorKind::AlreadyExists
    })
}

async fn drain_stream_events(
    rx: &mut mpsc::Receiver<Result<StreamEvent, tonic::Status>>,
    stream: &dyn StreamOutputPort,
) {
    loop {
        match rx.try_recv() {
            Ok(Ok(event)) => {
                let _ = stream.send(event).await;
            }
            Ok(Err(_)) => {}
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
            | Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                break;
            }
        }
    }
}

async fn drain_submit_events(
    rx: &mut mpsc::Receiver<Result<RunStreamEvent, tonic::Status>>,
    stream: &dyn RunStreamOutputPort,
) {
    loop {
        match rx.try_recv() {
            Ok(Ok(event)) => {
                let _ = stream.send(event).await;
            }
            Ok(Err(_)) => {}
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
            | Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                break;
            }
        }
    }
}

fn to_ssh_filters(filters: &[AppSyncFilterRule]) -> Vec<SyncFilterRule> {
    filters
        .iter()
        .map(|rule| SyncFilterRule {
            action: match rule.action {
                AppSyncFilterAction::Include => SyncFilterAction::Include,
                AppSyncFilterAction::Exclude => SyncFilterAction::Exclude,
            },
            pattern: rule.pattern.clone(),
        })
        .collect()
}

#[async_trait]
impl RemoteExecPort for SshAdapter {
    #[tracing::instrument(
        name = "ssh",
        level = "debug",
        skip(self, config, command),
        fields(op = "exec_capture", host = %config.host, user = %config.username, port = config.addr.port())
    )]
    async fn exec_capture(
        &self,
        config: &SshConfig,
        command: &str,
    ) -> AppResult<crate::app::ports::ExecCapture> {
        let session = self.sessions.get_or_create(config).await?;
        let (stdout, stderr, exit_code) = session
            .exec_capture(command)
            .await
            .map_err(map_exec_error)?;
        Ok(crate::app::ports::ExecCapture {
            stdout,
            stderr,
            exit_code,
        })
    }

    #[tracing::instrument(
        name = "ssh",
        level = "debug",
        skip(self, config, command, stream, mfa),
        fields(op = "exec_stream", host = %config.host, user = %config.username, port = config.addr.port())
    )]
    async fn exec_stream(
        &self,
        config: &SshConfig,
        command: &str,
        stream: &dyn StreamOutputPort,
        mfa: &mut dyn MfaPort,
    ) -> AppResult<()> {
        let session = self.sessions.get_or_create(config).await?;
        let (evt_tx, evt_rx) = mpsc::channel::<Result<StreamEvent, tonic::Status>>(64);
        let (mfa_tx, ssh_mfa_rx) = mpsc::channel::<MfaAnswer>(16);
        let mut mfa_tx = Some(mfa_tx);

        let mut evt_rx = evt_rx;
        let inbound_mfa = mfa.receiver();
        let mut exec_fut = Box::pin(session.exec(command, evt_tx, ssh_mfa_rx));

        loop {
            tokio::select! {
                res = &mut exec_fut => {
                    drain_stream_events(&mut evt_rx, stream).await;
                    return res.map_err(map_exec_error);
                }
                maybe_evt = evt_rx.recv() => {
                    if let Some(item) = maybe_evt {
                        if let Ok(event) = item {
                            let _ = stream.send(event).await;
                        }
                    }
                }
                maybe_mfa = inbound_mfa.recv() => {
                    match maybe_mfa {
                        Some(ans) => {
                            if let Some(tx) = mfa_tx.take() {
                                match tx.send(ans).await {
                                    Ok(()) => {
                                        mfa_tx = Some(tx);
                                    }
                                    Err(_) => {
                                        // Receiver closed; stop forwarding MFA.
                                    }
                                }
                            }
                        }
                        None => {
                            mfa_tx = None;
                        }
                    }
                }
            }
        }
    }

    #[tracing::instrument(
        name = "ssh",
        level = "debug",
        skip(self, config, stream, mfa),
        fields(op = "ensure_connected", host = %config.host, user = %config.username, port = config.addr.port())
    )]
    async fn ensure_connected(
        &self,
        config: &SshConfig,
        stream: &dyn StreamOutputPort,
        mfa: &mut dyn MfaPort,
    ) -> AppResult<()> {
        let session = self.sessions.get_or_create(config).await?;
        let (evt_tx, evt_rx) = mpsc::channel::<Result<StreamEvent, tonic::Status>>(64);
        let (mfa_tx, mut ssh_mfa_rx) = mpsc::channel::<MfaAnswer>(16);
        let mut mfa_tx = Some(mfa_tx);

        let mut evt_rx = evt_rx;
        let inbound_mfa = mfa.receiver();
        let mut ensure_fut = Box::pin(session.ensure_connected(&evt_tx, &mut ssh_mfa_rx));

        loop {
            tokio::select! {
                res = &mut ensure_fut => {
                    drain_stream_events(&mut evt_rx, stream).await;
                    return res.map_err(map_connect_error);
                }
                maybe_evt = evt_rx.recv() => {
                    if let Some(item) = maybe_evt {
                        if let Ok(event) = item {
                            let _ = stream.send(event).await;
                        }
                    }
                }
                maybe_mfa = inbound_mfa.recv() => {
                    match maybe_mfa {
                        Some(ans) => {
                            if let Some(tx) = mfa_tx.take() {
                                match tx.send(ans).await {
                                    Ok(()) => {
                                        mfa_tx = Some(tx);
                                    }
                                    Err(_) => {}
                                }
                            }
                        }
                        None => {
                            mfa_tx = None;
                        }
                    }
                }
            }
        }
    }

    #[tracing::instrument(
        name = "ssh",
        level = "debug",
        skip(self, config, stream, mfa),
        fields(op = "ensure_connected_submit", host = %config.host, user = %config.username, port = config.addr.port())
    )]
    async fn ensure_connected_submit(
        &self,
        config: &SshConfig,
        stream: &dyn RunStreamOutputPort,
        mfa: &mut dyn MfaPort,
    ) -> AppResult<()> {
        let session = self.sessions.get_or_create(config).await?;
        let (evt_tx, evt_rx) = mpsc::channel::<Result<RunStreamEvent, tonic::Status>>(64);
        let (mfa_tx, mut ssh_mfa_rx) = mpsc::channel::<MfaAnswer>(16);
        let mut mfa_tx = Some(mfa_tx);

        let mut evt_rx = evt_rx;
        let inbound_mfa = mfa.receiver();
        let mut ensure_fut = Box::pin(session.ensure_connected_submit(&evt_tx, &mut ssh_mfa_rx));

        loop {
            tokio::select! {
                res = &mut ensure_fut => {
                    drain_submit_events(&mut evt_rx, stream).await;
                    return res.map_err(map_connect_error);
                }
                maybe_evt = evt_rx.recv() => {
                    if let Some(item) = maybe_evt {
                        if let Ok(event) = item {
                            let _ = stream.send(event).await;
                        }
                    }
                }
                maybe_mfa = inbound_mfa.recv() => {
                    match maybe_mfa {
                        Some(ans) => {
                            if let Some(tx) = mfa_tx.take() {
                                match tx.send(ans).await {
                                    Ok(()) => {
                                        mfa_tx = Some(tx);
                                    }
                                    Err(_) => {}
                                }
                            }
                        }
                        None => {
                            mfa_tx = None;
                        }
                    }
                }
            }
        }
    }

    #[tracing::instrument(
        name = "ssh",
        level = "debug",
        skip(self, config),
        fields(op = "needs_connect", host = %config.host, user = %config.username, port = config.addr.port())
    )]
    async fn needs_connect(&self, config: &SshConfig) -> AppResult<bool> {
        let session = self.sessions.get_or_create(config).await?;
        Ok(session.needs_connect().await)
    }

    #[tracing::instrument(
        name = "ssh",
        level = "debug",
        skip(self, config, remote_dir),
        fields(op = "directory_exists", host = %config.host, user = %config.username, port = config.addr.port(), path = %remote_dir)
    )]
    async fn directory_exists(&self, config: &SshConfig, remote_dir: &str) -> AppResult<bool> {
        let session = self.sessions.get_or_create(config).await?;
        session
            .directory_exists(remote_dir)
            .await
            .map_err(map_directory_error)
    }

    #[tracing::instrument(
        name = "ssh",
        level = "debug",
        skip(self),
        fields(op = "is_connected", session = %session_name)
    )]
    async fn is_connected(&self, session_name: &str) -> AppResult<bool> {
        Ok(self.sessions.is_connected(session_name).await)
    }

    #[tracing::instrument(
        name = "ssh",
        level = "debug",
        skip(self),
        fields(op = "remove_session", session = %session_name)
    )]
    async fn remove_session(&self, session_name: &str) -> AppResult<bool> {
        Ok(self.sessions.remove_and_shutdown(session_name).await)
    }
}

#[async_trait]
impl FileSyncPort for SshAdapter {
    #[tracing::instrument(
        name = "sftp",
        level = "debug",
        skip(self, config, local_dir, remote_dir, options, stream, mfa),
        fields(op = "sync_dir", host = %config.host, user = %config.username, port = config.addr.port(), path = %remote_dir)
    )]
    async fn sync_dir(
        &self,
        config: &SshConfig,
        local_dir: &Path,
        remote_dir: &str,
        options: AppSyncOptions,
        stream: &dyn RunStreamOutputPort,
        mfa: &mut dyn MfaPort,
    ) -> AppResult<()> {
        let session = self.sessions.get_or_create(config).await?;
        let (evt_tx, evt_rx) = mpsc::channel::<Result<RunStreamEvent, tonic::Status>>(64);
        let (mfa_tx, ssh_mfa_rx) = mpsc::channel::<MfaAnswer>(16);
        let mut mfa_tx = Some(mfa_tx);
        let filters = to_ssh_filters(&options.filters);
        let sync_options = SyncOptions {
            block_size: options.block_size,
            parallelism: options.parallelism,
            filters: &filters,
        };

        let mut evt_rx = evt_rx;
        let inbound_mfa = mfa.receiver();
        let mut sync_fut =
            Box::pin(session.sync_dir(local_dir, remote_dir, sync_options, &evt_tx, ssh_mfa_rx));

        loop {
            tokio::select! {
                res = &mut sync_fut => {
                    drain_submit_events(&mut evt_rx, stream).await;
                    return res.map_err(|err| {
                        AppError::with_message(
                            AppErrorKind::Internal,
                            codes::REMOTE_ERROR,
                            format!("ssh sync failed: {err}"),
                        )
                    });
                }
                maybe_evt = evt_rx.recv() => {
                    if let Some(item) = maybe_evt {
                        if let Ok(event) = item {
                            let _ = stream.send(event).await;
                        }
                    }
                }
                maybe_mfa = inbound_mfa.recv() => {
                    match maybe_mfa {
                        Some(ans) => {
                            if let Some(tx) = mfa_tx.take() {
                                match tx.send(ans).await {
                                    Ok(()) => {
                                        mfa_tx = Some(tx);
                                    }
                                    Err(_) => {}
                                }
                            }
                        }
                        None => {
                            mfa_tx = None;
                        }
                    }
                }
            }
        }
    }

    #[tracing::instrument(
        name = "sftp",
        level = "debug",
        skip(self, config, remote_path, local_path),
        fields(op = "retrieve_path", host = %config.host, user = %config.username, port = config.addr.port(), path = %remote_path, overwrite = overwrite)
    )]
    async fn retrieve_path(
        &self,
        config: &SshConfig,
        remote_path: &str,
        local_path: &Path,
        overwrite: bool,
    ) -> AppResult<()> {
        let session = self.sessions.get_or_create(config).await?;
        match session
            .retrieve_path(remote_path, local_path, overwrite)
            .await
        {
            Ok(()) => Ok(()),
            Err(err) if is_sftp_missing_path(&err) => Err(AppError::with_message(
                AppErrorKind::InvalidArgument,
                codes::NOT_FOUND,
                format!("remote path missing: {remote_path}"),
            )),
            Err(err) if is_local_path_conflict(&err) => Err(AppError::with_message(
                AppErrorKind::Conflict,
                codes::CONFLICT,
                err.to_string(),
            )),
            Err(err) => Err(AppError::with_message(
                AppErrorKind::Internal,
                codes::REMOTE_ERROR,
                format!("remote retrieve failed: {err}"),
            )),
        }
    }
}
