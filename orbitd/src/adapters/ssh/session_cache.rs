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

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Default)]
    struct CountingFactory {
        builds: AtomicUsize,
    }

    impl CountingFactory {
        fn build_count(&self) -> usize {
            self.builds.load(Ordering::SeqCst)
        }
    }

    impl SessionFactory for CountingFactory {
        fn build(&self, params: SshParams) -> Arc<SessionManager> {
            self.builds.fetch_add(1, Ordering::SeqCst);
            Arc::new(SessionManager::new(params))
        }
    }

    #[tokio::test]
    async fn get_or_create_without_session_name_builds_new_each_time() {
        let factory = Arc::new(CountingFactory::default());
        let cache = SessionCache::new(factory.clone());
        let config = sample_config(None, "cluster-a", "alice");

        let first = cache
            .get_or_create(&config)
            .await
            .expect("config should be valid");
        let second = cache
            .get_or_create(&config)
            .await
            .expect("config should be valid");

        assert!(!Arc::ptr_eq(&first, &second));
        assert_eq!(factory.build_count(), 2);
        assert!(cache.get("cluster-a").await.is_none());
    }

    #[tokio::test]
    async fn get_or_create_reuses_matching_named_session() {
        let factory = Arc::new(CountingFactory::default());
        let cache = SessionCache::new(factory.clone());
        let config = sample_config(Some("primary"), "cluster-a", "alice");

        let first = cache
            .get_or_create(&config)
            .await
            .expect("config should be valid");
        let second = cache
            .get_or_create(&config)
            .await
            .expect("config should be valid");

        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(factory.build_count(), 1);
    }

    #[tokio::test]
    async fn get_or_create_replaces_named_session_when_params_change() {
        let factory = Arc::new(CountingFactory::default());
        let cache = SessionCache::new(factory.clone());
        let first_config = sample_config(Some("primary"), "cluster-a", "alice");
        let second_config = sample_config(Some("primary"), "cluster-b", "alice");

        let first = cache
            .get_or_create(&first_config)
            .await
            .expect("first config should be valid");
        let second = cache
            .get_or_create(&second_config)
            .await
            .expect("second config should be valid");
        let stored = cache
            .get("primary")
            .await
            .expect("session should be cached under name");

        assert!(!Arc::ptr_eq(&first, &second));
        assert!(Arc::ptr_eq(&second, &stored));
        assert_eq!(factory.build_count(), 2);
    }

    #[tokio::test]
    async fn is_connected_returns_false_for_missing_session() {
        let cache = SessionCache::new(Arc::new(DefaultSessionFactory));
        assert!(!cache.is_connected("missing").await);
    }

    #[test]
    fn to_params_rejects_blank_username() {
        let config = sample_config(Some("primary"), "cluster-a", "   ");
        let err = to_params(&config).expect_err("blank username should fail");
        assert_eq!(err.kind(), AppErrorKind::InvalidArgument);
        assert_eq!(err.code(), codes::INVALID_ARGUMENT);
    }

    #[test]
    fn to_params_rejects_blank_host() {
        let config = sample_config(Some("primary"), "   ", "alice");
        let err = to_params(&config).expect_err("blank host should fail");
        assert_eq!(err.kind(), AppErrorKind::InvalidArgument);
        assert_eq!(err.code(), codes::INVALID_ARGUMENT);
    }

    #[test]
    fn to_params_maps_valid_config() {
        let config = sample_config(Some("primary"), "cluster-a", "alice");
        let params = to_params(&config).expect("config should map");
        assert_eq!(params.host, "cluster-a");
        assert_eq!(
            params.addr,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 2222)
        );
        assert_eq!(params.username, "alice");
        assert_eq!(params.identity_path.as_deref(), Some("~/.ssh/id_ed25519"));
        assert_eq!(params.ki_submethods.as_deref(), Some("pam"));
        assert_eq!(params.keepalive_secs, 15);
    }

    fn sample_config(session_name: Option<&str>, host: &str, username: &str) -> SshConfig {
        SshConfig {
            session_name: session_name.map(ToOwned::to_owned),
            host: host.to_string(),
            addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 2222),
            username: username.to_string(),
            identity_path: Some("~/.ssh/id_ed25519".to_string()),
            ki_submethods: Some("pam".to_string()),
            keepalive_secs: 15,
        }
    }
}
