// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::RwLock;

use crate::app::errors::{AppError, AppErrorKind, AppResult, codes};
use crate::app::types::SshConfig;

use super::{SessionManager, SshParams};

pub trait SessionFactory: Send + Sync {
    fn build(&self, params: SshParams) -> Arc<SessionManager>;
}

#[derive(Default)]
pub struct DefaultSessionFactory;

impl SessionFactory for DefaultSessionFactory {
    fn build(&self, params: SshParams) -> Arc<SessionManager> {
        Arc::new(SessionManager::new(params))
    }
}

pub struct SessionCache {
    sessions: RwLock<HashMap<String, Arc<SessionManager>>>,
    factory: Arc<dyn SessionFactory>,
}

impl SessionCache {
    pub fn new(factory: Arc<dyn SessionFactory>) -> Self {
        Self {
            sessions: RwLock::new(HashMap::new()),
            factory,
        }
    }

    pub async fn get(&self, name: &str) -> Option<Arc<SessionManager>> {
        self.sessions.read().await.get(name).cloned()
    }

    pub async fn insert(&self, name: String, session: Arc<SessionManager>) {
        self.sessions.write().await.insert(name, session);
    }

    pub async fn remove_and_shutdown(&self, name: &str) -> bool {
        let session = self.sessions.write().await.remove(name);
        if let Some(session) = session {
            session.shutdown().await;
            return true;
        }
        false
    }

    pub async fn is_connected(&self, name: &str) -> bool {
        let Some(session) = self.get(name).await else {
            return false;
        };
        session.is_connected_nonblocking()
    }

    pub async fn get_or_create(&self, config: &SshConfig) -> AppResult<Arc<SessionManager>> {
        let params = to_params(config)?;
        let Some(session_name) = config.session_name.as_deref() else {
            return Ok(self.factory.build(params));
        };

        if let Some(existing) = self.get(session_name).await {
            if existing.matches_params(&params) {
                return Ok(existing);
            }
        }

        let session = self.factory.build(params);
        self.insert(session_name.to_string(), session.clone()).await;
        Ok(session)
    }
}

fn to_params(config: &SshConfig) -> AppResult<SshParams> {
    if config.username.trim().is_empty() {
        return Err(AppError::new(
            AppErrorKind::InvalidArgument,
            codes::INVALID_ARGUMENT,
        ));
    }
    if config.host.trim().is_empty() {
        return Err(AppError::new(
            AppErrorKind::InvalidArgument,
            codes::INVALID_ARGUMENT,
        ));
    }
    Ok(SshParams {
        host: config.host.clone(),
        addr: config.addr,
        username: config.username.clone(),
        identity_path: config.identity_path.clone(),
        ki_submethods: config.ki_submethods.clone(),
        keepalive_secs: config.keepalive_secs,
    })
}
