// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

use anyhow::anyhow;
#[cfg(test)]
use anyhow::Result;
#[cfg(test)]
use proto::{MfaAnswer, SubmitStreamEvent};
use russh::client::Config;
use russh::keys::known_hosts::{learn_known_hosts, learn_known_hosts_path};
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
#[cfg(test)]
use tokio::sync::mpsc;

#[cfg(test)]
use crate::ssh::sync::BoxFuture;

mod auth;
mod exec;
mod sftp;

#[cfg(test)]
mod tests;

/// Minimal russh client handler. We rely on default implementations.
#[derive(Clone, Debug)]
struct ClientHandler {
    host: String,
    addr: SocketAddr,
}

impl ClientHandler {
    fn new(host: String, addr: SocketAddr) -> Self {
        Self { host, addr }
    }
}

impl russh::client::Handler for ClientHandler {
    type Error = anyhow::Error;
    async fn check_server_key(
        &mut self,
        server_public_key: &russh::keys::ssh_key::PublicKey,
    ) -> std::result::Result<bool, Self::Error> {
        verify_server_key(&self.host, self.addr, server_public_key, None)
    }
}

/// Parameters for establishing the SSH connection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SshParams {
    /// Original hostname or IP used for known_hosts lookup.
    pub host: String,
    pub addr: SocketAddr,
    pub username: String,
    pub identity_path: Option<String>,
    /// Preferred submethods hint for keyboard-interactive (often unused by servers).
    pub ki_submethods: Option<String>,
    /// Send TCP keepalives to keep long connections healthy.
    pub keepalive_secs: u64,
}

fn check_known_hosts_for(
    host: &str,
    port: u16,
    key: &russh::keys::ssh_key::PublicKey,
    known_hosts_path: Option<&Path>,
) -> std::result::Result<bool, russh::keys::Error> {
    match known_hosts_path {
        Some(path) => russh::keys::check_known_hosts_path(host, port, key, path),
        None => russh::keys::check_known_hosts(host, port, key),
    }
}

fn learn_known_hosts_for(
    host: &str,
    port: u16,
    key: &russh::keys::ssh_key::PublicKey,
    known_hosts_path: Option<&Path>,
) -> std::result::Result<(), russh::keys::Error> {
    match known_hosts_path {
        Some(path) => learn_known_hosts_path(host, port, key, path),
        None => learn_known_hosts(host, port, key),
    }
}

fn verify_server_key(
    host: &str,
    addr: SocketAddr,
    key: &russh::keys::ssh_key::PublicKey,
    known_hosts_path: Option<&Path>,
) -> std::result::Result<bool, anyhow::Error> {
    let port = addr.port();
    match check_known_hosts_for(host, port, key, known_hosts_path) {
        Ok(true) => return Ok(true),
        Ok(false) => {}
        Err(err) => {
            log::warn!("server key validation failed for {host}:{port}: {err}");
            return Err(anyhow!(
                "server key validation failed for {host}:{port}: {err}"
            ));
        }
    }

    let ip_host = addr.ip().to_string();
    if ip_host != host {
        match check_known_hosts_for(&ip_host, port, key, known_hosts_path) {
            Ok(true) => return Ok(true),
            Ok(false) => {}
            Err(err) => {
                log::warn!("server key validation failed for {host}:{port}: {err}");
                return Err(anyhow!(
                    "server key validation failed for {host}:{port}: {err}"
                ));
            }
        }
    }

    let tried = if ip_host == host {
        host.to_string()
    } else {
        format!("{host}, {ip_host}")
    };
    log::info!(
        "server key for {host}:{port} is not present in known_hosts (tried {tried}); learning"
    );
    learn_known_hosts_for(host, port, key, known_hosts_path).map_err(|err| {
        log::warn!("failed to learn server key for {host}:{port}: {err}");
        anyhow!("failed to learn server key for {host}:{port}: {err}")
    })?;
    Ok(true)
}

#[cfg(test)]
pub(crate) struct SessionManagerTestHooks {
    ensure_connected: Arc<
        dyn Fn(
                &mpsc::Sender<Result<SubmitStreamEvent, tonic::Status>>,
                &mut mpsc::Receiver<MfaAnswer>,
            ) -> BoxFuture<'static, Result<()>>
            + Send
            + Sync,
    >,
    ensure_remote_dir: Arc<dyn Fn(&str) -> BoxFuture<'static, Result<()>> + Send + Sync>,
    sync_one_file:
        Arc<dyn Fn(&Path, &str, &str, usize) -> BoxFuture<'static, Result<()>> + Send + Sync>,
}

/// Manager that owns a single long-lived SSH connection.
pub struct SessionManager {
    params: SshParams,
    config: Arc<Config>,
    // The active handle, protected by a mutex because we serialize command use
    handle: Arc<Mutex<Option<russh::client::Handle<ClientHandler>>>>,
    // Background keepalive task
    keepalive_task_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    #[cfg(test)]
    test_hooks: Option<SessionManagerTestHooks>,
}

impl SessionManager {
    pub fn new(params: SshParams) -> Self {
        let cfg = Config {
            inactivity_timeout: Some(Duration::from_secs(30)),
            keepalive_interval: Some(Duration::from_secs(params.keepalive_secs)),
            // reasonable channel buffer and window sizes for streaming
            channel_buffer_size: 64,
            window_size: 1024 * 1024,
            ..Default::default()
        };
        Self {
            params,
            config: Arc::new(cfg),
            handle: Arc::new(Mutex::new(None)),
            keepalive_task_handle: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            test_hooks: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn set_test_hooks(&mut self, hooks: SessionManagerTestHooks) {
        self.test_hooks = Some(hooks);
    }

    pub async fn needs_connect(&self) -> bool {
        let handle_field = self.handle.lock().await;
        match handle_field.as_ref() {
            None => true,
            Some(h) if h.is_closed() => true,
            Some(_) => false,
        }
    }

    pub fn is_connected_nonblocking(&self) -> bool {
        let Ok(handle_field) = self.handle.try_lock() else {
            return false;
        };
        match handle_field.as_ref() {
            None => false,
            Some(h) if h.is_closed() => false,
            Some(_) => true,
        }
    }

    pub fn matches_params(&self, params: &SshParams) -> bool {
        self.params == *params
    }

    pub async fn shutdown(&self) {
        if let Some(task) = self.keepalive_task_handle.lock().await.take() {
            task.abort();
        }
        let mut handle_field = self.handle.lock().await;
        let _ = handle_field.take();
    }
}
