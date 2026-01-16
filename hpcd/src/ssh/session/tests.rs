// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

use super::{SessionManager, SessionManagerTestHooks};
use crate::ssh::sync::{SyncFilterRule, SyncOptions, sync_dir_with_executor};
use anyhow::Result;
use std::fs;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::{Arc, Mutex};
use tempfile::tempdir;
use tokio::sync::mpsc;

#[tokio::test]
async fn session_manager_executor_uses_test_hooks() {
    let tmp = tempdir().unwrap();
    let root = tmp.path();

    fs::create_dir_all(root.join("src")).unwrap();
    fs::write(root.join("src/lib.rs"), "lib").unwrap();

    let calls: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));

    let connect_calls = Arc::clone(&calls);
    let ensure_connected = Arc::new(
        move |_: &mpsc::Sender<Result<proto::SubmitStreamEvent, tonic::Status>>,
              _: &mut mpsc::Receiver<proto::MfaAnswer>| {
            let connect_calls = Arc::clone(&connect_calls);
            let fut: crate::ssh::sync::BoxFuture<'static, Result<()>> = Box::pin(async move {
                connect_calls.lock().unwrap().push("connect".to_string());
                Ok(())
            });
            fut
        },
    );

    let mkdir_calls = Arc::clone(&calls);
    let ensure_remote_dir = Arc::new(move |remote_dir: &str| {
        let mkdir_calls = Arc::clone(&mkdir_calls);
        let remote_dir = remote_dir.to_string();
        let fut: crate::ssh::sync::BoxFuture<'static, Result<()>> = Box::pin(async move {
            mkdir_calls
                .lock()
                .unwrap()
                .push(format!("mkdir:{remote_dir}"));
            Ok(())
        });
        fut
    });

    let sync_calls = Arc::clone(&calls);
    let sync_one_file = Arc::new(
        move |_local: &Path, remote: &str, _session_id: &str, _block_size: usize| {
            let sync_calls = Arc::clone(&sync_calls);
            let remote = remote.to_string();
            let fut: crate::ssh::sync::BoxFuture<'static, Result<()>> = Box::pin(async move {
                sync_calls.lock().unwrap().push(format!("sync:{remote}"));
                Ok(())
            });
            fut
        },
    );

    let hooks = SessionManagerTestHooks {
        ensure_connected,
        ensure_remote_dir,
        sync_one_file,
    };

    let params = super::SshParams {
        host: "127.0.0.1".to_string(),
        addr: "127.0.0.1:22".parse::<SocketAddr>().unwrap(),
        username: "test".to_string(),
        identity_path: None,
        ki_submethods: None,
        keepalive_secs: 1,
    };
    let mut manager = SessionManager::new(params);
    manager.set_test_hooks(hooks);

    let (evt_tx, _evt_rx) = mpsc::channel::<Result<proto::SubmitStreamEvent, tonic::Status>>(1);
    let (_mfa_tx, mfa_rx) = mpsc::channel::<proto::MfaAnswer>(1);
    let options = SyncOptions {
        block_size: None,
        parallelism: Some(1),
        filters: &[] as &[SyncFilterRule],
    };

    sync_dir_with_executor(&manager, root, "/remote", options, &evt_tx, mfa_rx)
        .await
        .unwrap();

    let calls = calls.lock().unwrap().clone();
    assert!(calls.contains(&"connect".to_string()));
    assert!(calls.contains(&"mkdir:/remote".to_string()));
    assert!(calls.contains(&"mkdir:/remote/src".to_string()));
    assert!(calls.contains(&"sync:/remote/src/lib.rs".to_string()));
}

#[tokio::test]
async fn needs_connect_true_without_handle() {
    let params = super::SshParams {
        host: "127.0.0.1".to_string(),
        addr: "127.0.0.1:22".parse::<SocketAddr>().unwrap(),
        username: "test".to_string(),
        identity_path: None,
        ki_submethods: None,
        keepalive_secs: 1,
    };
    let manager = SessionManager::new(params);
    assert!(manager.needs_connect().await);
}

#[test]
fn verify_server_key_accepts_known_host() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("known_hosts");
    let key_b64 =
        "AAAAC3NzaC1lZDI1NTE5AAAAIJdD7y3aLq454yWBdwLWbieU1ebz9/cu7/QEXn9OIeZJ";
    fs::write(
        &path,
        format!("example.com ssh-ed25519 {key_b64}\n"),
    )
    .unwrap();
    let key = russh::keys::parse_public_key_base64(key_b64).unwrap();
    let addr = "203.0.113.10:22".parse::<SocketAddr>().unwrap();

    let ok = super::verify_server_key("example.com", addr, &key, Some(&path)).unwrap();
    assert!(ok);
}

#[test]
fn verify_server_key_accepts_ip_fallback() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("known_hosts");
    let key_b64 =
        "AAAAC3NzaC1lZDI1NTE5AAAAIJdD7y3aLq454yWBdwLWbieU1ebz9/cu7/QEXn9OIeZJ";
    fs::write(&path, format!("203.0.113.10 ssh-ed25519 {key_b64}\n")).unwrap();
    let key = russh::keys::parse_public_key_base64(key_b64).unwrap();
    let addr = "203.0.113.10:22".parse::<SocketAddr>().unwrap();

    let ok = super::verify_server_key("example.com", addr, &key, Some(&path)).unwrap();
    assert!(ok);
}

#[test]
fn verify_server_key_learns_unknown_host() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("known_hosts");
    let key_b64 =
        "AAAAC3NzaC1lZDI1NTE5AAAAIJdD7y3aLq454yWBdwLWbieU1ebz9/cu7/QEXn9OIeZJ";
    let key = russh::keys::parse_public_key_base64(key_b64).unwrap();
    let addr = "203.0.113.10:22".parse::<SocketAddr>().unwrap();

    let ok = super::verify_server_key("missing.example.com", addr, &key, Some(&path)).unwrap();
    assert!(ok);

    let contents = fs::read_to_string(&path).unwrap();
    assert!(contents.contains("missing.example.com"));
    assert!(contents.contains(key_b64));
}

#[test]
fn verify_server_key_rejects_changed_key() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("known_hosts");
    let key_b64 =
        "AAAAC3NzaC1lZDI1NTE5AAAAIJdD7y3aLq454yWBdwLWbieU1ebz9/cu7/QEXn9OIeZJ";
    let changed_key_b64 =
        "AAAAC3NzaC1lZDI1NTE5AAAAILIG2T/B0l0gaqj3puu510tu9N1OkQ4znY3LYuEm5zCF";
    fs::write(&path, format!("example.com ssh-ed25519 {key_b64}\n")).unwrap();
    let changed_key = russh::keys::parse_public_key_base64(changed_key_b64).unwrap();
    let addr = "203.0.113.10:22".parse::<SocketAddr>().unwrap();

    let err = super::verify_server_key("example.com", addr, &changed_key, Some(&path));
    assert!(err.is_err());
}
