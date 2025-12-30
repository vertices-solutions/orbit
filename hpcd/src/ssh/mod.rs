use anyhow::{Context, Result, anyhow};
use futures_util::StreamExt;
use proto::{MfaAnswer, MfaPrompt, Prompt, StreamEvent, stream_event};
use rand::{Rng, distr::Alphanumeric};
use russh::ChannelMsg;
use russh::client::Config;
use russh::client::{Handler, KeyboardInteractiveAuthResponse};
use russh::keys::PrivateKeyWithHashAlg;
use russh_sftp::client::SftpSession;
use russh_sftp::protocol::{FileAttributes, OpenFlags};
use sha2::{Digest, Sha256};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::time::UNIX_EPOCH;
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::fs as tokiofs;
use tokio::io::AsyncReadExt;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio::sync::{Mutex, mpsc};
use tokio_stream::wrappers::ReceiverStream;
mod sync_plan;

use sync_plan::build_sync_plan;
/// Minimal russh client handler. We rely on default implementations.
/// TODO: add actual server key verification
#[derive(Clone, Debug, Default)]
pub struct ClientHandler;

impl Handler for ClientHandler {
    type Error = anyhow::Error;
    async fn check_server_key(
        &mut self,
        _server_public_key: &russh::keys::ssh_key::PublicKey,
    ) -> std::result::Result<bool, Self::Error> {
        Ok(true)
    }
}

/// Parameters for establishing the SSH connection.
#[derive(Clone, Debug)]
pub struct SshParams {
    pub addr: SocketAddr,
    pub username: String,
    pub identity_path: Option<String>,
    /// Preferred submethods hint for keyboard-interactive (often unused by servers).
    pub ki_submethods: Option<String>,
    /// Send TCP keepalives to keep long connections healthy.
    pub keepalive_secs: u64,
}

#[derive(Clone, Copy, Debug)]
pub enum SyncFilterAction {
    Include,
    Exclude,
}

#[derive(Clone, Debug)]
pub struct SyncFilterRule {
    pub action: SyncFilterAction,
    pub pattern: String,
}

#[derive(Clone, Copy, Debug)]
pub struct SyncOptions<'a> {
    pub block_size: Option<usize>,
    pub parallelism: Option<usize>,
    pub filters: &'a [SyncFilterRule],
}

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Abstraction over the remote side of the sync so it can be mocked in tests.
pub(crate) trait SyncExecutor: Sync {
    /// Ensure the remote connection is established and MFA prompts can flow.
    fn ensure_connected<'a>(
        &'a self,
        evt_tx: &'a mpsc::Sender<Result<StreamEvent, tonic::Status>>,
        mfa_rx: &'a mut mpsc::Receiver<MfaAnswer>,
    ) -> BoxFuture<'a, Result<()>>;

    /// Ensure the given remote directory exists.
    fn ensure_remote_dir<'a>(&'a self, remote_dir: &'a str) -> BoxFuture<'a, Result<()>>;

    /// Sync one local file to its remote destination.
    fn sync_one_file<'a>(
        &'a self,
        local_path: &'a Path,
        remote_path: &'a str,
        session_id: &'a str,
        block_size: usize,
    ) -> BoxFuture<'a, Result<()>>;
}

#[cfg(test)]
struct SessionManagerTestHooks {
    ensure_connected: Arc<
        dyn Fn(
                &mpsc::Sender<Result<StreamEvent, tonic::Status>>,
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
    fn set_test_hooks(&mut self, hooks: SessionManagerTestHooks) {
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
    /// Ensure we have a connected & authenticated handle.
    /// Streams any keyboard-interactive MFA prompts to `evt_tx`
    /// and consumes responses from `mfa_rx`.
    pub async fn ensure_connected(
        &self,
        evt_tx: &mpsc::Sender<Result<StreamEvent, tonic::Status>>,
        mfa_rx: &mut mpsc::Receiver<MfaAnswer>,
    ) -> Result<()> {
        let mut handle_field = self.handle.lock().await;

        // If handle exists but is closed, drop it so we reconnect.
        let needs_connect = match handle_field.as_ref() {
            None => true,
            Some(h) if h.is_closed() => true,
            Some(_) => false,
        };

        if needs_connect {
            log::info!(
                "re-establishing connection with {}@{}",
                &self.params.username,
                &self.params.addr
            );
            // Establish TCP + SSH
            let handler = ClientHandler;
            let mut handle = russh::client::connect(self.config.clone(), self.params.addr, handler)
                .await
                .context("SSH connect failed")?;
            log::info!(
                "established initial connection with {}@{}, proceeding with auth",
                &self.params.username,
                &self.params.addr
            );
            // Try publickey first if identity provided
            if let Some(path) = &self.params.identity_path {
                let key = russh::keys::load_secret_key(path, None)
                    .with_context(|| format!("failed to load secret key at {}", path))?;
                let key = Arc::new(key);
                // Prefer SHA-256 for RSA if applicable (ignored for non-RSA keys)
                let pk = PrivateKeyWithHashAlg::new(
                    key,
                    handle.best_supported_rsa_hash().await?.flatten(),
                );
                match handle
                    .authenticate_publickey(self.params.username.clone(), pk)
                    .await?
                {
                    russh::client::AuthResult::Success => {
                        // Auth finished, good to go
                    }
                    russh::client::AuthResult::Failure {
                        remaining_methods,
                        partial_success,
                    } if partial_success
                        && remaining_methods.contains(&russh::MethodKind::KeyboardInteractive) =>
                    {
                        // Fall back to KI
                        self.do_keyboard_interactive(&mut handle, evt_tx, mfa_rx)
                            .await?;
                    }
                    russh::client::AuthResult::Failure {
                        remaining_methods,
                        partial_success,
                    } => {
                        return Err(anyhow!(
                            "authentication failure(remaining_methods={:?},partial_success={:?})",
                            remaining_methods,
                            partial_success
                        ));
                    }
                }
            } else {
                // No key -> go straight to keyboard interactive
                self.do_keyboard_interactive(&mut handle, evt_tx, mfa_rx)
                    .await?;
            }

            *handle_field = Some(handle);
            // Start a keepalive pinger in the background
            if let Some(interval) = self.config.keepalive_interval {
                let handle_clone = self.handle.clone();
                let want_reply = true;
                let jh = tokio::spawn(async move {
                    let mut ticker = tokio::time::interval(interval / 2);
                    loop {
                        ticker.tick().await;
                        let guard = handle_clone.lock().await;
                        let Some(handle) = guard.as_ref() else {
                            continue;
                        };
                        if handle.is_closed() {
                            log::debug!("keepalive handle is closed");
                            break;
                        }
                        if let Err(e) = handle.send_keepalive(want_reply).await {
                            log::debug!("error when sending a keepalive: {}", e);
                        } else {
                            log::debug!("successfully sent a keepalive message");
                        }
                    }
                });

                *self.keepalive_task_handle.lock().await = Some(jh);
            }
        } else {
            log::info!(
                "don't need to re-establish connection to {}@{}",
                &self.params.username,
                &self.params.addr
            );
        }

        Ok(())
    }

    async fn ensure_connected_for_sync(
        &self,
        evt_tx: &mpsc::Sender<Result<StreamEvent, tonic::Status>>,
        mfa_rx: &mut mpsc::Receiver<MfaAnswer>,
    ) -> Result<()> {
        #[cfg(test)]
        if let Some(hooks) = &self.test_hooks {
            return (hooks.ensure_connected)(evt_tx, mfa_rx).await;
        }
        self.ensure_connected(evt_tx, mfa_rx).await
    }

    /// Runs the keyboard-interactive auth loop, streaming prompts out
    /// and consuming answers from mfa_rx until Success/Failure.
    async fn do_keyboard_interactive(
        &self,
        handle: &mut russh::client::Handle<ClientHandler>,
        evt_tx: &mpsc::Sender<Result<StreamEvent, tonic::Status>>,
        mfa_rx: &mut mpsc::Receiver<MfaAnswer>,
    ) -> Result<()> {
        let mut ki = handle
            .authenticate_keyboard_interactive_start(
                self.params.username.clone(),
                self.params.ki_submethods.clone(),
            )
            .await
            .context("KI start failed")?;

        loop {
            match ki {
                KeyboardInteractiveAuthResponse::Success => return Ok(()),
                KeyboardInteractiveAuthResponse::Failure {
                    remaining_methods,
                    partial_success,
                } => {
                    // Auth failed. Surface a clear error to client.
                    let msg = format!(
                        "authentication failed (partial_success={}, remaining={:?})",
                        partial_success, remaining_methods
                    );
                    return Err(anyhow!(msg));
                }
                KeyboardInteractiveAuthResponse::InfoRequest {
                    name,
                    instructions,
                    prompts,
                } => {
                    // Stream MFA prompt to the client
                    let prompt_msg = MfaPrompt {
                        name,
                        instructions,
                        prompts: prompts
                            .into_iter()
                            .map(|p| Prompt {
                                text: p.prompt,
                                echo: p.echo,
                            })
                            .collect(),
                    };
                    let _ = evt_tx
                        .send(Ok(StreamEvent {
                            event: Some(stream_event::Event::Mfa(prompt_msg)),
                        }))
                        .await;

                    // Wait for client answers
                    let answers = mfa_rx
                        .recv()
                        .await
                        .ok_or_else(|| anyhow!("client disconnected during MFA"))?;

                    // Respond to the server and continue the KI loop
                    ki = handle
                        .authenticate_keyboard_interactive_respond(answers.responses)
                        .await
                        .context("KI respond failed")?;
                }
            }
        }
    }
    async fn exec_simple(&self, cmd: &str) -> Result<i32> {
        // Executes a command with "dummy" channels.
        // TODO: Refactor cases when this is needed.
        let guard = self.handle.lock().await;
        let handle = guard.as_ref().ok_or_else(|| anyhow!("SSH handle lost"))?;
        let mut chan = handle.channel_open_session().await?;
        chan.exec(true, cmd).await?;
        let mut code: i32 = 0;
        while let Some(msg) = chan.wait().await {
            if let ChannelMsg::ExitStatus { exit_status } = msg {
                code = exit_status as i32;
            }
            if matches!(msg, ChannelMsg::Close) {
                break;
            }
        }
        let _ = chan.eof().await;
        let _ = chan.close().await;
        Ok(code)
    }

    // Execute command over SSH, retrieving stdout, stderr and exit code as output
    pub async fn exec_capture(&self, cmd: &str) -> Result<(Vec<u8>, Vec<u8>, i32)> {
        let guard = self.handle.lock().await;
        let handle = guard.as_ref().ok_or_else(|| anyhow!("SSH handle lost"))?;
        let mut chan = handle.channel_open_session().await?;
        let actual_command = cmd;
        log::debug!("executing '{}'", &actual_command);
        //r#"bash -lc 'echo "$SHELL"; echo "$PATH"; command -v python3; python3 -V'"#;
        chan.exec(true, actual_command)
            .await
            .context("exec request")?;
        //chan.eof().await?;
        let mut out = Vec::new();
        let mut err = Vec::new();
        let mut code: i32 = 0;
        loop {
            let Some(msg) = chan.wait().await else {
                break;
            };
            match msg {
                ChannelMsg::Data { ref data } => {
                    out.extend_from_slice(data);
                }
                ChannelMsg::ExtendedData { ref data, ext: 1 } => {
                    err.extend_from_slice(data)
                }
                ChannelMsg::ExitStatus { exit_status } => code = exit_status as i32,

                ChannelMsg::Close => break,

                _ => {}
            }
        }

        let _ = chan.close().await;
        Ok((out, err, code))
    }

    /// Execute a single command over the (shared) SSH connection,
    /// streaming stdout/stderr and exit code to `evt_tx`.
    ///
    /// Only one command is run at a time; a long-lived channel lock ensures that.
    pub async fn exec(
        &self,
        cmd: &str,
        evt_tx: mpsc::Sender<Result<StreamEvent, tonic::Status>>,
        mut mfa_rx: mpsc::Receiver<MfaAnswer>,
    ) -> Result<()> {
        // Enforce whitelist

        // Ensure connection & (re)authenticate if needed (may prompt MFA)
        self.ensure_connected(&evt_tx, &mut mfa_rx).await?;

        // From here on, hold the handle lock for the duration of the command
        let guard = self.handle.lock().await;
        let handle = guard
            .as_ref()
            .ok_or_else(|| anyhow!("SSH handle lost after connect"))?;

        let mut chan = handle
            .channel_open_session()
            .await
            .context("open session")?;

        chan.exec(true, cmd).await.context("exec request")?;

        // Read the remote process output and forward as gRPC stream items
        while let Some(msg) = chan.wait().await {
            match msg {
                ChannelMsg::Data { data } => {
                    let _ = evt_tx
                        .send(Ok(StreamEvent {
                            event: Some(stream_event::Event::Stdout(data.to_vec())),
                        }))
                        .await;
                }
                ChannelMsg::ExtendedData { data, ext } => {
                    // SSH ext_type 1 == STDERR
                    if ext == 1 {
                        let _ = evt_tx
                            .send(Ok(StreamEvent {
                                event: Some(stream_event::Event::Stderr(data.to_vec())),
                            }))
                            .await;
                    }
                }
                ChannelMsg::ExitStatus { exit_status } => {
                    let _ = evt_tx
                        .send(Ok(StreamEvent {
                            event: Some(stream_event::Event::ExitCode(exit_status as i32)),
                        }))
                        .await;
                }
                ChannelMsg::Eof => {
                    // ignore; loop will break after close/exit
                }
                ChannelMsg::Close => {
                    break;
                }
                _ => {
                    // Ignore other messages for this simple exec flow
                }
            }
        }

        // Be tidy
        let _ = chan.eof().await;
        let _ = chan.close().await;

        Ok(())
    }
    async fn sftp(&self) -> Result<SftpSession> {
        // TODO: this function is using dummy auth channels right now - for production use, they
        // MUST be hooked to user-guided MFA
        let guard = self.handle.lock().await;
        let handle = guard
            .as_ref()
            .ok_or_else(|| anyhow!("SSH handle lost before opening SFTP"))?;
        let channel = handle.channel_open_session().await?;
        channel.request_subsystem(true, "sftp").await?;
        let sftp = SftpSession::new(channel.into_stream()).await?;
        Ok(sftp)
    }

    async fn ensure_remote_dir_for_sync(&self, remote_dir: &str) -> Result<()> {
        #[cfg(test)]
        if let Some(hooks) = &self.test_hooks {
            return (hooks.ensure_remote_dir)(remote_dir).await;
        }
        let sftp = self.sftp().await?;
        self.ensure_remote_dir_with_sftp(&sftp, remote_dir).await
    }
    async fn ensure_remote_dir_with_sftp(
        &self,
        sftp_session: &SftpSession,
        remote_dir: &str,
    ) -> Result<()> {
        // Normalize to components and create incrementally
        let mut cur = String::from("");
        log::debug!("remote dir: {remote_dir}");
        for comp in Path::new(remote_dir).components() {
            let seg = match comp {
                std::path::Component::RootDir => "/".to_string(),
                std::path::Component::Normal(os) => os.to_string_lossy().to_string(),
                std::path::Component::CurDir => continue,
                std::path::Component::ParentDir => continue, // TODO: this actually needs to remove component, I will need to fix this
                _ => continue,
            };
            log::debug!("seg: {}", &seg);
            if seg.is_empty() {
                log::debug!("empty seg");
                continue;
            }
            if cur != "/" && !cur.is_empty() {
                cur = format!("{}/{}", cur.trim_end_matches("/"), seg);
            } else {
                cur = format!("/{}", seg);
            }
            log::debug!("cur: {}", &cur);
            match sftp_session.metadata(&cur).await {
                Ok(meta) => {
                    if !meta.is_dir() {
                        return Err(anyhow!(
                            "remote path exists but is not a directory: {}",
                            cur
                        ));
                    }
                }
                Err(e) => {
                    log::debug!("got error when retrieving metadata: {}", e);
                    let attrs = FileAttributes {
                        permissions: Some(0o700),
                        ..Default::default()
                    };
                    sftp_session
                        .create_dir(&cur)
                        .await
                        .context(format!("creating path {}", &cur))?;
                    match sftp_session.set_metadata(&cur, attrs).await {
                        Ok(_) => {}
                        Err(e) => {
                            log::warn!("error when setting metadata for path {}: {}", &cur, e)
                        }
                    };
                }
            }
        }
        Ok(())
    }

    /// checks if remote hasher already exists on remote
    /// if it does - exit and do nothing
    /// if it doesn't - writes file to remote
    /// TODO - we don't need to do this within sync_one_file
    async fn ensure_remote_temp_exe(
        &self,
        content: &[u8],
        session_id: &str,
        suffix: &str,
    ) -> Result<String> {
        let sftp = self.sftp().await?;
        let path = format!(
            "/tmp/hpcd_{}.{}",
            session_id,
            suffix.trim_start_matches('.')
        );
        if sftp.try_exists(&path).await? {
            return Ok(path);
        }
        // Create & write
        let flags = OpenFlags::WRITE.union(OpenFlags::CREATE);
        let attrs = FileAttributes {
            permissions: Some(0o700),
            ..Default::default()
        };
        let mut f = sftp
            .open_with_flags_and_attributes(&path, flags, attrs)
            .await
            .with_context(|| format!("open remote temp {}", &path))?;
        f.write(content)
            .await
            .with_context(|| format!("write remote temp {}", &path))?;
        f.flush().await?;
        f.shutdown().await?;
        Ok(path)
    }

    /// computes hashes of blocks on a remote file
    /// Using a Python helper script.
    ///
    async fn remote_block_hashes(
        &self,
        remote_path: &str,
        session_id: &str,
        block_size: usize,
    ) -> Result<Vec<String>> {
        let escaped_path = remote_path.replace("\\", "\\\\").replace("'", "\\'");
        let quoted_path = format!("'{escaped_path}'");
        let py_script = format!(
            r#"#!/usr/bin/env python3
import sys, os, hashlib
path = r{rp}
bs = {bs}
try:
    st = os.stat(path)
except Exception as e:
    print("ERR", e, file=sys.stderr)
    sys.exit(2)
h = hashlib.sha256
off = 0
with open(path, 'rb') as f:
    while True:
        b = f.read(bs)
        if not b: break
        print(off, len(b), h(b).hexdigest(), file=sys.stdout)
        off += len(b)
sys.stdout.flush()
"#,
            rp = quoted_path,
            bs = block_size
        );
        // TODO: ensure_remote_path_exe can be run once per transfer, no need to do this for every
        // file: even if script on remote exists, it will require some network trafic to check if
        // it exists.
        let script_path = self
            .ensure_remote_temp_exe(py_script.as_bytes(), session_id, "py")
            .await?;
        let cmd_py3 = format!("python3 -u {}", &script_path);
        let (out, err, code) = self.exec_capture(&cmd_py3).await?;
        let (out, err, code) = if code != 0 {
            let cmd_py = format!("python -u {}", &script_path);
            self.exec_capture(&cmd_py).await?
        } else {
            (out, err, code)
        };
        // 3) Cleanup temp script (best-effort)
        //let _ = self.sftp().await?.remove_file(&script_path).await;
        if code != 0 {
            let err_message = String::from_utf8(err)?;
            return Err(anyhow!(err_message));
        }
        let out = String::from_utf8(out)?;
        let mut hashes = Vec::new();
        for line in out.lines() {
            if line.starts_with("ERR ") {
                return Err(anyhow!("remote: {}", line));
            }
            let mut it = line.split_whitespace();
            let _off = it.next();
            let _len = it.next();
            let hex = it.next();
            if let Some(h) = hex {
                hashes.push(h.to_string());
            }
        }
        Ok(hashes)
    }
    /// Syncs a single file from source to destination.
    /// Is kinda stable, but definitely needs more testing.
    async fn sync_one_file_with_sftp(
        &self,
        sftp: &SftpSession,
        local_path: &Path,
        remote_path: &str,
        session_id: &str,
        block_size: usize,
    ) -> Result<()> {
        let lmeta = tokiofs::metadata(local_path).await?;
        let lsize = lmeta.len();
        let lmtime = lmeta
            .modified()
            .unwrap_or(UNIX_EPOCH)
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let mut rmeta = match sftp.metadata(remote_path).await {
            Ok(v) => v,
            Err(e) => {
                match e {
                    russh_sftp::client::error::Error::Status(ref estatus) => {
                        match estatus.status_code {
                            russh_sftp::protocol::StatusCode::NoSuchFile => {
                                // This is a good case - we just need to transfer file and exit,
                                // that's it
                                log::debug!("file {} doesn't exist, uploading", remote_path);
                                return upload_single_file(
                                    sftp,
                                    &local_path.to_path_buf(),
                                    remote_path,
                                    block_size,
                                )
                                .await;
                            }
                            _ => {
                                log::warn!("encountered Status error with status {:?}", estatus);
                                anyhow::bail!("encountered Status error with status {:?}", estatus)
                            }
                        }
                    }
                    /*
                    russh_sftp::client::error::Error::IO(ref ioerror) => {
                        log::debug!("IO error: {}", ioerror);
                    }
                    russh_sftp::client::error::Error::Timeout => {
                        log::debug!("encountered timeout error");
                    }
                    russh_sftp::client::error::Error::Limited(ref limiterror) => {
                        log::debug!("Limit exceeded error: {}", limiterror);
                    }
                    russh_sftp::client::error::Error::UnexpectedPacket => {
                        log::debug!("Unexpected packet");
                    }
                    russh_sftp::client::error::Error::UnexpectedBehavior(ref uerror) => {
                        log::debug!("Unexpected behavior: {}", uerror);
                    }
                    */
                    _ => anyhow::bail!("encountered error: {}", e),
                };
            }
        };
        if rmeta.is_dir() {
            anyhow::bail!("{} is a directory", remote_path);
        }
        if let Some(rmtime) = rmeta.mtime {
            if rmtime as u64 >= lmtime {
                log::debug!(
                    "remote file {remote_path} is at least as new as local file {local_path:?}, skipping transfer",
                );

                return Ok(());
            }
        } else {
            log::debug!("could not get mtime for remote path {remote_path}")
        }

        if rmeta.is_empty() {
            // Remote file is empty - weird but OK, we'll just upload our local file.
            return upload_single_file(sftp, &local_path.to_path_buf(), remote_path, block_size)
                .await;
        }
        log::debug!("Opening remote file for random-access writes");
        // Try to open remote file for random-access writes (create if absent)
        let flags = OpenFlags::WRITE.union(OpenFlags::READ);
        /*
            .union(OpenFlags::APPEND)
            .union(OpenFlags::TRUNCATE);
        */
        let mut rfile = sftp
            .open_with_flags(remote_path, flags)
            .await
            .context("opening remote file for random-access writes")?;
        // Plan: compute remote block hashes (without downloading file data) by running
        // a tiny Python hashing script *on the remote* over an exec channel.
        // If that fails (python missing, permission issues, etc), fall back to full upload.
        let remote_hashes = match self
            .remote_block_hashes(remote_path, session_id, block_size)
            .await
        {
            Ok(h) => Some(h),
            Err(e) => {
                log::warn!(
                    "remote hash plan unavailable for {}: {} ; falling back to full upload",
                    remote_path,
                    e
                );
                None
            }
        };
        if let Some(rblocks) = remote_hashes {
            // Compute local block hashes
            let lblocks = local_block_hashes(local_path, block_size).await?;

            // Compare and send only differing blocks
            for (i, lbh) in lblocks.iter().enumerate() {
                let offset = (i as u64) * (block_size as u64);
                let remaining = lsize.saturating_sub(offset);
                let this_block = remaining.min(block_size as u64) as usize;

                let differing = match rblocks.get(i) {
                    Some(rh) => rh != lbh,
                    None => true, // remote shorter; need to append
                };

                if differing {
                    log::debug!("found differing block at offset {offset}, index {i}");
                    // Read local block and write to remote at offset
                    let mut buf = vec![0u8; this_block];
                    let mut f = tokiofs::File::open(local_path).await.context(format!(
                        "opening local file {}",
                        local_path.to_string_lossy()
                    ))?;
                    f.seek(std::io::SeekFrom::Start(offset))
                        .await
                        .context(format!("seeking in remote file at offset {offset}"))?;
                    let n = f.read(&mut buf[..]).await.context(format!(
                        "reading from {:?} at offset {}",
                        local_path, offset
                    ))?;
                    // TODO: in case of errors, fallback to doing full transfer
                    rfile
                        .seek(std::io::SeekFrom::Start(offset))
                        .await
                        .context(format!("seeking in remote file at offset {offset}"))?;
                    rfile
                        .write(&buf[..n])
                        .await
                        .with_context(|| format!("writing block @{} to {}", offset, remote_path))?;
                    rfile.flush().await?;
                }
            }

            // Truncate/extend remote file to match local size if needed
            // this is done in case the local file was truncated
            if rblocks.len() as u64 * block_size as u64 != lsize {
                log::info!("setting length medatata on remote file");
                // Use SFTP fsetstat(size) when supported; otherwise remote 'truncate'
                rmeta.size = Some(lsize);
                if let Err(_e) = rfile.set_metadata(rmeta).await {
                    log::warn!("setting metadata on remote failed - falling back to truncate");
                    // fallback via small exec
                    let cmd = format!("truncate -s {} {}", lsize, sh_escape(remote_path));
                    let _ = self.exec_simple(&cmd).await;
                }
            }
        } else {
            return upload_single_file(sftp, &local_path.to_path_buf(), remote_path, block_size)
                .await;
        }

        /*
        // Try to set remote mtime to local mtime so next run can skip
        log::debug!("setting remote mtime equal to local mtime");
        let mut attrs = FileAttributes::default();
        attrs.mtime = Some(lmtime as u32);
        let _ = sftp.set_metadata(remote_path, attrs).await;
        */
        Ok(())
    }

    async fn sync_one_file_for_sync(
        &self,
        local_path: &Path,
        remote_path: &str,
        session_id: &str,
        block_size: usize,
    ) -> Result<()> {
        #[cfg(test)]
        if let Some(hooks) = &self.test_hooks {
            return (hooks.sync_one_file)(local_path, remote_path, session_id, block_size).await;
        }
        let sftp = self.sftp().await?;
        self.sync_one_file_with_sftp(&sftp, local_path, remote_path, session_id, block_size)
            .await
    }

    /// Sync an entire local directory tree into a remote directory.
    /// - Creates remote directories as needed
    /// - Skips files where remote mtime >= local mtime
    /// - Uses block-delta writes for changed files
    pub async fn sync_dir<P: AsRef<Path>>(
        &self,
        local_dir: P,
        remote_dir: &str,
        options: SyncOptions<'_>,
        evt_tx: &mpsc::Sender<Result<StreamEvent, tonic::Status>>,
        mfa_rx: mpsc::Receiver<MfaAnswer>,
    ) -> Result<()> {
        sync_dir_with_executor(
            self,
            local_dir,
            remote_dir,
            options,
            evt_tx,
            mfa_rx,
        )
        .await
    }
    pub async fn retrieve_path(&self, remote_path: &str, local_path: &Path) -> Result<()> {
        let sftp = self.sftp().await?;
        let meta = sftp.metadata(remote_path).await?;
        if meta.is_dir() {
            download_dir(&sftp, remote_path, local_path).await
        } else {
            download_file(&sftp, remote_path, local_path).await
        }
    }
    pub async fn directory_exists(&self, dirname: &str) -> Result<bool, String> {
        let command = format!("ls {} 1>&2 2>/dev/null", dirname);
        let (_, _, code) = match self.exec_capture(&command).await {
            Ok(v) => v,
            Err(e) => return Err(e.to_string()),
        };
        match code {
            0 => Ok(true),
            _ => Ok(false),
        }
    }
}

async fn sync_dir_with_executor<E, P>(
    executor: &E,
    local_dir: P,
    remote_dir: &str,
    options: SyncOptions<'_>,
    evt_tx: &mpsc::Sender<Result<StreamEvent, tonic::Status>>,
    mut mfa_rx: mpsc::Receiver<MfaAnswer>,
) -> Result<()>
where
    E: SyncExecutor + ?Sized,
    P: AsRef<Path>,
{
    let block_size = options.block_size.unwrap_or(1024 * 1024); // 1 MiB block size
    let parallelism = options.parallelism.unwrap_or(8);

    executor.ensure_connected(evt_tx, &mut mfa_rx).await?;
    let plan = build_sync_plan(local_dir, remote_dir, options.filters)?;

    log::info!("making sure the remote directory exists");
    executor.ensure_remote_dir(remote_dir).await?;
    for remote_dir in &plan.remote_dirs {
        executor.ensure_remote_dir(remote_dir).await?;
    }

    let session_id: String = rand::rng()
        .sample_iter(&Alphanumeric)
        .take(10)
        .map(char::from)
        .collect();

    let results = futures::stream::iter(plan.items.into_iter().map(|item| {
        let session_id_clone = session_id.clone();
        async move {
            executor
                .sync_one_file(
                    &item.local_path,
                    &item.remote_path,
                    &session_id_clone,
                    block_size,
                )
                .await
        }
    }))
    .buffer_unordered(parallelism)
    .collect::<Vec<_>>()
    .await;
    let mut errs = Vec::new();
    for res in results {
        if let Err(e) = res {
            errs.push(e);
        }
    }
    if !errs.is_empty() {
        // Build a helpful combined error message.
        use std::fmt::Write as _;
        let mut msg = String::new();
        for (i, e) in errs.iter().enumerate() {
            let _ = writeln!(&mut msg, "[{}] {:#}", i + 1, e);
        }
        anyhow::bail!("sync_dir encountered {} error(s):\n{}", errs.len(), msg);
    }
    Ok(())
}

impl SyncExecutor for SessionManager {
    fn ensure_connected<'a>(
        &'a self,
        evt_tx: &'a mpsc::Sender<Result<StreamEvent, tonic::Status>>,
        mfa_rx: &'a mut mpsc::Receiver<MfaAnswer>,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move { self.ensure_connected_for_sync(evt_tx, mfa_rx).await })
    }

    fn ensure_remote_dir<'a>(&'a self, remote_dir: &'a str) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move { self.ensure_remote_dir_for_sync(remote_dir).await })
    }

    fn sync_one_file<'a>(
        &'a self,
        local_path: &'a Path,
        remote_path: &'a str,
        session_id: &'a str,
        block_size: usize,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            self.sync_one_file_for_sync(local_path, remote_path, session_id, block_size)
                .await
        })
    }
}

/// Helper to wrap an mpsc receiver as a tonic stream.
pub fn receiver_to_stream(
    rx: mpsc::Receiver<Result<StreamEvent, tonic::Status>>,
) -> ReceiverStream<Result<StreamEvent, tonic::Status>> {
    ReceiverStream::new(rx)
}

/// Compute local per-block hashes (BLAKE3, 16-byte) for a file.
async fn local_block_hashes(path: &Path, block_size: usize) -> Result<Vec<String>> {
    let mut f = tokiofs::File::open(path).await?;
    let size = f.metadata().await?.len();
    let capacity = size.div_ceil(block_size as u64) as usize;
    let mut out = Vec::with_capacity(capacity);
    let mut buf = vec![0u8; block_size];
    loop {
        let n = f.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        let mut h = Sha256::new();
        h.update(&buf[..n]);
        let hash = h.finalize();
        // 16 bytes (similar to remote B2b-128) to keep lines small
        out.push(format!("{:x}", hash));
    }
    Ok(out)
}

/// Very small, safe-ish shell escaper for paths.
pub(crate) fn sh_escape(p: &str) -> String {
    let mut out = String::from("'");
    out.push_str(&p.replace('\'', r"'\''"));
    out.push('\'');
    out
}

async fn upload_single_file(
    sftp: &SftpSession,
    local_path: &PathBuf,
    remote_path: &str,
    block_size: usize,
) -> anyhow::Result<()> {
    // File just doesn't exist yet we simply copy it over SFTP
    log::debug!(
        "Uploading file over sftp: {} -> {}",
        local_path.to_string_lossy(),
        remote_path
    );
    let mut lf = tokiofs::File::open(local_path).await?;
    // Removed flag APPEND because it's not needed
    let flags = OpenFlags::READ
        .union(OpenFlags::WRITE)
        .union(OpenFlags::CREATE);
    let mut rfile = sftp.open_with_flags(remote_path, flags).await?;
    let mut offset = 0u64;
    let mut buf = vec![0u8; block_size];
    loop {
        let n = lf.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        rfile.seek(std::io::SeekFrom::Start(offset)).await?;
        rfile.write_all(&buf[..n]).await?;
        offset += n as u64;
    }
    rfile.flush().await?;
    Ok(())
}

async fn download_file(sftp: &SftpSession, remote_path: &str, local_path: &Path) -> Result<()> {
    if let Some(parent) = local_path.parent() {
        tokiofs::create_dir_all(parent).await?;
    }
    let mut rfile = sftp.open(remote_path).await?;
    let mut lfile = tokiofs::File::create(local_path).await?;
    tokio::io::copy(&mut rfile, &mut lfile).await?;
    lfile.flush().await?;
    Ok(())
}

async fn download_dir(sftp: &SftpSession, remote_dir: &str, local_dir: &Path) -> Result<()> {
    let mut stack: Vec<(String, PathBuf)> = vec![(
        remote_dir.trim_end_matches('/').to_string(),
        local_dir.to_path_buf(),
    )];

    while let Some((remote_base, local_base)) = stack.pop() {
        tokiofs::create_dir_all(&local_base).await?;
        let entries = sftp.read_dir(&remote_base).await?;
        for entry in entries {
            let name = entry.file_name();
            let remote_child = format!("{}/{}", remote_base, name);
            let local_child = local_base.join(&name);
            let meta = entry.metadata();
            if meta.is_dir() {
                stack.push((remote_child, local_child));
            } else {
                download_file(sftp, &remote_child, &local_child).await?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod executor_tests {
    use super::{
        MfaAnswer, StreamEvent, SyncExecutor, SyncFilterAction, SyncFilterRule, SyncOptions,
        sync_dir_with_executor,
    };
    use anyhow::{Result, anyhow};
    use std::collections::HashSet;
    use std::fs;
    use std::path::Path;
    use std::sync::{Arc, Mutex};
    use tempfile::tempdir;
    use tokio::sync::mpsc;

    #[derive(Clone, Default)]
    struct FakeExecutor {
        calls: Arc<Mutex<Vec<String>>>,
        fail_on: HashSet<String>,
    }

    impl FakeExecutor {
        fn new(fail_on: &[&str]) -> Self {
            Self {
                calls: Arc::new(Mutex::new(Vec::new())),
                fail_on: fail_on.iter().map(|s| s.to_string()).collect(),
            }
        }

        fn calls(&self) -> Vec<String> {
            self.calls.lock().unwrap().clone()
        }
    }

    impl SyncExecutor for FakeExecutor {
        fn ensure_connected<'a>(
            &'a self,
            _evt_tx: &'a mpsc::Sender<Result<StreamEvent, tonic::Status>>,
            _mfa_rx: &'a mut mpsc::Receiver<MfaAnswer>,
        ) -> super::BoxFuture<'a, Result<()>> {
            let calls = Arc::clone(&self.calls);
            Box::pin(async move {
                calls.lock().unwrap().push("connect".to_string());
                Ok(())
            })
        }

        fn ensure_remote_dir<'a>(
            &'a self,
            remote_dir: &'a str,
        ) -> super::BoxFuture<'a, Result<()>> {
            let calls = Arc::clone(&self.calls);
            let remote_dir = remote_dir.to_string();
            Box::pin(async move {
                calls.lock().unwrap().push(format!("mkdir:{remote_dir}"));
                Ok(())
            })
        }

        fn sync_one_file<'a>(
            &'a self,
            _local_path: &'a Path,
            remote_path: &'a str,
            _session_id: &'a str,
            _block_size: usize,
        ) -> super::BoxFuture<'a, Result<()>> {
            let calls = Arc::clone(&self.calls);
            let remote_path = remote_path.to_string();
            let fail_on = self.fail_on.clone();
            Box::pin(async move {
                calls.lock().unwrap().push(format!("sync:{remote_path}"));
                if fail_on.contains(&remote_path) {
                    return Err(anyhow!("forced error for {remote_path}"));
                }
                Ok(())
            })
        }
    }

    fn rule(action: SyncFilterAction, pattern: &str) -> SyncFilterRule {
        SyncFilterRule {
            action,
            pattern: pattern.to_string(),
        }
    }

    #[tokio::test]
    async fn sync_dir_calls_remote_dirs_before_sync() {
        let tmp = tempdir().unwrap();
        let root = tmp.path();

        fs::create_dir_all(root.join("src/bin")).unwrap();
        fs::write(root.join("README.md"), "readme").unwrap();
        fs::write(root.join("src/lib.rs"), "lib").unwrap();
        fs::write(root.join("src/bin/main.rs"), "bin").unwrap();

        let executor = FakeExecutor::default();
        let (evt_tx, _evt_rx) = mpsc::channel::<Result<StreamEvent, tonic::Status>>(1);
        let (_mfa_tx, mfa_rx) = mpsc::channel::<MfaAnswer>(1);
        let filters = [rule(SyncFilterAction::Exclude, "target/")];
        let options = SyncOptions {
            block_size: None,
            parallelism: Some(1),
            filters: &filters,
        };

        sync_dir_with_executor(
            &executor,
            root,
            "/remote",
            options,
            &evt_tx,
            mfa_rx,
        )
        .await
        .unwrap();

        let calls = executor.calls();
        let expected_prefix = vec![
            "connect",
            "mkdir:/remote",
            "mkdir:/remote/src",
            "mkdir:/remote/src/bin",
        ];
        assert!(calls.len() >= expected_prefix.len());
        assert_eq!(calls[..expected_prefix.len()], expected_prefix);
    }

    #[tokio::test]
    async fn sync_dir_aggregates_sync_errors() {
        let tmp = tempdir().unwrap();
        let root = tmp.path();

        fs::create_dir_all(root.join("src")).unwrap();
        fs::write(root.join("src/lib.rs"), "lib").unwrap();
        fs::write(root.join("src/extra.rs"), "extra").unwrap();

        let executor = FakeExecutor::new(&["/remote/src/lib.rs"]);
        let (evt_tx, _evt_rx) = mpsc::channel::<Result<StreamEvent, tonic::Status>>(1);
        let (_mfa_tx, mfa_rx) = mpsc::channel::<MfaAnswer>(1);
        let options = SyncOptions {
            block_size: None,
            parallelism: Some(1),
            filters: &[],
        };

        let err = sync_dir_with_executor(
            &executor,
            root,
            "/remote",
            options,
            &evt_tx,
            mfa_rx,
        )
        .await
        .unwrap_err();

        let msg = format!("{err:#}");
        assert!(msg.contains("sync_dir encountered 1 error(s):"));
        assert!(msg.contains("forced error for /remote/src/lib.rs"));
    }
}

#[cfg(test)]
mod session_manager_executor_tests {
    use super::{
        MfaAnswer, SessionManager, SessionManagerTestHooks, StreamEvent, SyncFilterRule, SyncOptions,
        sync_dir_with_executor,
    };
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
            move |_: &mpsc::Sender<Result<StreamEvent, tonic::Status>>,
                  _: &mut mpsc::Receiver<MfaAnswer>| {
                let connect_calls = Arc::clone(&connect_calls);
                let fut: super::BoxFuture<'static, Result<()>> = Box::pin(async move {
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
            let fut: super::BoxFuture<'static, Result<()>> = Box::pin(async move {
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
                let fut: super::BoxFuture<'static, Result<()>> = Box::pin(async move {
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
            addr: "127.0.0.1:22".parse::<SocketAddr>().unwrap(),
            username: "test".to_string(),
            identity_path: None,
            ki_submethods: None,
            keepalive_secs: 1,
        };
        let mut manager = SessionManager::new(params);
        manager.set_test_hooks(hooks);

        let (evt_tx, _evt_rx) = mpsc::channel::<Result<StreamEvent, tonic::Status>>(1);
        let (_mfa_tx, mfa_rx) = mpsc::channel::<MfaAnswer>(1);
        let options = SyncOptions {
            block_size: None,
            parallelism: Some(1),
            filters: &[] as &[SyncFilterRule],
        };

        sync_dir_with_executor(
            &manager,
            root,
            "/remote",
            options,
            &evt_tx,
            mfa_rx,
        )
        .await
        .unwrap();

        let calls = calls.lock().unwrap().clone();
        assert!(calls.contains(&"connect".to_string()));
        assert!(calls.contains(&"mkdir:/remote".to_string()));
        assert!(calls.contains(&"mkdir:/remote/src".to_string()));
        assert!(calls.contains(&"sync:/remote/src/lib.rs".to_string()));
    }
}
