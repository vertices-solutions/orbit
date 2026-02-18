// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::{Duration as StdDuration, Instant};

use proto::{
    RunResult, RunStatus, RunStreamEvent, StreamEvent, run_result, run_status, run_stream_event,
    stream_event,
};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use time::format_description::well_known::Rfc3339;
use time::{Duration as TimeDuration, OffsetDateTime};
use tokio::sync::{mpsc, watch};
use tokio::time::{Duration as TokioDuration, sleep};
use walkdir::WalkDir;

use crate::app::errors::{AppError, AppErrorKind, AppResult, codes};
use crate::app::ports::{
    BlueprintStorePort, ClockPort, ClusterStorePort, FileSyncPort, JobStorePort,
    LocalFilesystemPort, MfaPort, NetworkProbePort, RemoteExecPort, RunStreamOutputPort,
    StreamOutputPort, TelemetryEvent, TelemetryPort,
};
use crate::app::services::{
    managers, os, project_building, random, remote_path, run_paths, sbatch, shell, slurm, templates,
};
use crate::app::types::{
    Address, BlueprintRecord, ClusterStatus, HostRecord, JobRecord, NewBlueprintBuild, NewHost,
    NewJob, SshConfig, SyncOptions,
};

const CLEANUP_CANCEL_POLL_INTERVAL: TokioDuration = TokioDuration::from_secs(2);
const CLEANUP_CANCEL_TIMEOUT: TokioDuration = TokioDuration::from_secs(60);
const ORBITFILE_NAME: &str = "Orbitfile";
const LIST_PARTITIONS_COMMAND: &str = "scontrol show partition -o";
const LIST_ACCOUNTS_COMMAND: &str = "sshare -U -P -u $USER --format Account --noheader";
const TARBALL_HASH_FUNCTION_BLAKE3: &str = "blake3";

#[derive(Clone)]
pub struct UseCases {
    pub(crate) clusters: std::sync::Arc<dyn ClusterStorePort>,
    pub(crate) jobs: std::sync::Arc<dyn JobStorePort>,
    pub(crate) projects: std::sync::Arc<dyn BlueprintStorePort>,
    pub(crate) remote_exec: std::sync::Arc<dyn RemoteExecPort>,
    pub(crate) file_sync: std::sync::Arc<dyn FileSyncPort>,
    pub(crate) local_fs: std::sync::Arc<dyn LocalFilesystemPort>,
    pub(crate) network: std::sync::Arc<dyn NetworkProbePort>,
    pub(crate) clock: std::sync::Arc<dyn ClockPort>,
    pub(crate) telemetry: std::sync::Arc<dyn TelemetryPort>,
    pub(crate) tarballs_dir: PathBuf,
}

impl UseCases {
    pub fn new(
        clusters: std::sync::Arc<dyn ClusterStorePort>,
        jobs: std::sync::Arc<dyn JobStorePort>,
        projects: std::sync::Arc<dyn BlueprintStorePort>,
        remote_exec: std::sync::Arc<dyn RemoteExecPort>,
        file_sync: std::sync::Arc<dyn FileSyncPort>,
        local_fs: std::sync::Arc<dyn LocalFilesystemPort>,
        network: std::sync::Arc<dyn NetworkProbePort>,
        clock: std::sync::Arc<dyn ClockPort>,
        telemetry: std::sync::Arc<dyn TelemetryPort>,
        tarballs_dir: PathBuf,
    ) -> Self {
        Self {
            clusters,
            jobs,
            projects,
            remote_exec,
            file_sync,
            local_fs,
            network,
            clock,
            telemetry,
            tarballs_dir,
        }
    }

    pub async fn ping(&self, message: &str) -> AppResult<String> {
        if message.trim() == "ping" {
            Ok("pong".to_string())
        } else {
            Err(AppError::new(
                AppErrorKind::InvalidArgument,
                codes::INVALID_ARGUMENT,
            ))
        }
    }

    pub async fn list_clusters(&self, check_reachability: bool) -> AppResult<Vec<ClusterStatus>> {
        let hosts = self.clusters.list_hosts(None).await?;
        let mut out = Vec::with_capacity(hosts.len());
        for host in hosts {
            let reachable = if check_reachability {
                match self
                    .network
                    .check_host_reachable(&host.address, host.port)
                    .await
                {
                    Ok(value) => value,
                    Err(err) => {
                        tracing::debug!(
                            "reachability check failed name={} host={} error={}",
                            host.name,
                            format_address(&host.address),
                            err
                        );
                        false
                    }
                }
            } else {
                false
            };
            let connected = if check_reachability && !reachable {
                false
            } else {
                match self.remote_exec.is_connected(&host.name).await {
                    Ok(value) => value,
                    Err(err) => {
                        tracing::debug!(
                            "connectivity check failed name={} host={} error={}",
                            host.name,
                            format_address(&host.address),
                            err
                        );
                        false
                    }
                }
            };
            out.push(ClusterStatus {
                host,
                connected,
                reachable,
            });
        }
        Ok(out)
    }

    pub async fn list_jobs(
        &self,
        name_filter: Option<&str>,
        blueprint_filter: Option<&str>,
    ) -> AppResult<Vec<JobRecord>> {
        let cluster_name = name_filter.map(str::to_string);
        let blueprint_name = normalize_blueprint_filter(blueprint_filter)?;

        let mut jobs = if let Some(name) = name_filter {
            let Some(host) = self.clusters.get_by_name(name).await? else {
                return Err(AppError::new(
                    AppErrorKind::InvalidArgument,
                    codes::NOT_FOUND,
                ));
            };
            self.jobs.list_jobs_for_host(host.id).await?
        } else {
            self.jobs.list_all_jobs().await?
        };

        if let Some(filter) = blueprint_name.as_deref() {
            jobs.retain(|job| job.blueprint_name.as_deref() == Some(filter));
        }

        self.telemetry.event(
            "jobs.listed",
            TelemetryEvent {
                cluster: cluster_name,
                project: blueprint_name,
                ..TelemetryEvent::default()
            },
        );
        Ok(jobs)
    }

    pub async fn list_partitions(&self, name: &str) -> AppResult<Vec<String>> {
        let cluster_name = name.trim();
        if cluster_name.is_empty() {
            return Err(AppError::new(
                AppErrorKind::InvalidArgument,
                codes::INVALID_ARGUMENT,
            ));
        }

        let host = match self.clusters.get_by_name(cluster_name).await? {
            Some(host) => host,
            None => {
                return Err(AppError::with_message(
                    AppErrorKind::NotFound,
                    codes::NOT_FOUND,
                    format!("cluster '{cluster_name}' not found"),
                ));
            }
        };

        let config = self.config_for_host(&host).await?;
        if self
            .remote_exec
            .needs_connect(&config)
            .await
            .unwrap_or(true)
        {
            let stream = NoopStreamOutput;
            let (mfa_tx, mfa_rx) = mpsc::channel::<proto::MfaAnswer>(1);
            drop(mfa_tx);
            let mut mfa_port = GrpcMfaPort { receiver: mfa_rx };
            self.remote_exec
                .ensure_connected(&config, &stream, &mut mfa_port)
                .await
                .map_err(|err| {
                    AppError::with_message(
                        AppErrorKind::Aborted,
                        codes::REMOTE_ERROR,
                        format!("failed to connect to cluster '{cluster_name}': {err}"),
                    )
                })?;
        }

        let capture = self
            .remote_exec
            .exec_capture(&config, LIST_PARTITIONS_COMMAND)
            .await
            .map_err(|err| {
                AppError::with_message(
                    AppErrorKind::Aborted,
                    codes::REMOTE_ERROR,
                    format!("failed to enumerate partitions on '{cluster_name}': {err}"),
                )
            })?;

        if capture.exit_code != 0 {
            let stderr = String::from_utf8_lossy(&capture.stderr);
            let detail = stderr.trim();
            return Err(AppError::with_message(
                AppErrorKind::Aborted,
                codes::REMOTE_ERROR,
                if detail.is_empty() {
                    format!(
                        "partition enumeration command failed on '{cluster_name}' with exit code {}",
                        capture.exit_code
                    )
                } else {
                    format!(
                        "partition enumeration command failed on '{cluster_name}' with exit code {}: {}",
                        capture.exit_code, detail
                    )
                },
            ));
        }

        let output = String::from_utf8(capture.stdout).map_err(|err| {
            AppError::with_message(
                AppErrorKind::Internal,
                codes::REMOTE_ERROR,
                format!("failed to decode partition output from '{cluster_name}': {err}"),
            )
        })?;

        let parsed = slurm::parse_scontrol_partitions(&output).map_err(|err| {
            AppError::with_message(
                AppErrorKind::Internal,
                codes::REMOTE_ERROR,
                format!("failed to parse partition output from '{cluster_name}': {err}"),
            )
        })?;

        Ok(parsed.into_iter().map(|partition| partition.name).collect())
    }

    pub async fn list_accounts(&self, name: &str) -> AppResult<Vec<String>> {
        let cluster_name = name.trim();
        if cluster_name.is_empty() {
            return Err(AppError::new(
                AppErrorKind::InvalidArgument,
                codes::INVALID_ARGUMENT,
            ));
        }

        let host = match self.clusters.get_by_name(cluster_name).await? {
            Some(host) => host,
            None => {
                return Err(AppError::with_message(
                    AppErrorKind::NotFound,
                    codes::NOT_FOUND,
                    format!("cluster '{cluster_name}' not found"),
                ));
            }
        };

        let config = self.config_for_host(&host).await?;
        if self
            .remote_exec
            .needs_connect(&config)
            .await
            .unwrap_or(true)
        {
            let stream = NoopStreamOutput;
            let (mfa_tx, mfa_rx) = mpsc::channel::<proto::MfaAnswer>(1);
            drop(mfa_tx);
            let mut mfa_port = GrpcMfaPort { receiver: mfa_rx };
            self.remote_exec
                .ensure_connected(&config, &stream, &mut mfa_port)
                .await
                .map_err(|err| {
                    AppError::with_message(
                        AppErrorKind::Aborted,
                        codes::REMOTE_ERROR,
                        format!("failed to connect to cluster '{cluster_name}': {err}"),
                    )
                })?;
        }

        let capture = self
            .remote_exec
            .exec_capture(&config, LIST_ACCOUNTS_COMMAND)
            .await
            .map_err(|err| {
                AppError::with_message(
                    AppErrorKind::Aborted,
                    codes::REMOTE_ERROR,
                    format!("failed to enumerate accounts on '{cluster_name}': {err}"),
                )
            })?;

        if capture.exit_code != 0 {
            let stderr = String::from_utf8_lossy(&capture.stderr);
            let detail = stderr.trim();
            return Err(AppError::with_message(
                AppErrorKind::Aborted,
                codes::REMOTE_ERROR,
                if detail.is_empty() {
                    format!(
                        "account enumeration command failed on '{cluster_name}' with exit code {}",
                        capture.exit_code
                    )
                } else {
                    format!(
                        "account enumeration command failed on '{cluster_name}' with exit code {}: {}",
                        capture.exit_code, detail
                    )
                },
            ));
        }

        let output = String::from_utf8(capture.stdout).map_err(|err| {
            AppError::with_message(
                AppErrorKind::Internal,
                codes::REMOTE_ERROR,
                format!("failed to decode account output from '{cluster_name}': {err}"),
            )
        })?;

        Ok(slurm::parse_sshare_accounts(&output))
    }

    pub async fn connect_cluster(
        &self,
        name: &str,
        stream: &dyn StreamOutputPort,
        mfa: &mut dyn MfaPort,
    ) -> AppResult<()> {
        let cluster_name = name.trim();
        if cluster_name.is_empty() {
            return Err(AppError::new(
                AppErrorKind::InvalidArgument,
                codes::INVALID_ARGUMENT,
            ));
        }

        let host = match self.clusters.get_by_name(cluster_name).await? {
            Some(host) => host,
            None => {
                return Err(AppError::with_message(
                    AppErrorKind::NotFound,
                    codes::NOT_FOUND,
                    format!("cluster '{cluster_name}' not found"),
                ));
            }
        };
        let config = self.config_for_host(&host).await?;

        if self.remote_exec.needs_connect(&config).await? {
            self.remote_exec
                .ensure_connected(&config, stream, mfa)
                .await?;
        } else {
            self.remote_exec.send_keepalive(&host.name).await?;
        }

        self.telemetry.event(
            "cluster.connected",
            TelemetryEvent {
                cluster: Some(host.name),
                user: Some(host.username),
                ..TelemetryEvent::default()
            },
        );
        Ok(())
    }

    pub async fn upsert_blueprint(&self, name: &str) -> AppResult<BlueprintRecord> {
        let blueprint = self.projects.upsert_blueprint(name).await?;
        self.telemetry.event(
            "project.upserted",
            TelemetryEvent {
                project: Some(blueprint.name.clone()),
                ..TelemetryEvent::default()
            },
        );
        Ok(blueprint)
    }

    pub async fn get_blueprint_by_name(&self, name: &str) -> AppResult<BlueprintRecord> {
        let trimmed = name.trim();
        if let Some((blueprint_name, tag)) = split_blueprint_ref(trimmed) {
            if tag == "latest" {
                return self.get_latest_blueprint_build(blueprint_name).await;
            }
        }
        match self.projects.get_blueprint_by_name(trimmed).await? {
            Some(project) => Ok(project),
            None => Err(AppError::new(
                AppErrorKind::InvalidArgument,
                codes::NOT_FOUND,
            )),
        }
    }

    pub async fn list_blueprints(&self) -> AppResult<Vec<BlueprintRecord>> {
        let blueprints = self.projects.list_blueprints().await?;
        self.telemetry
            .event("projects.listed", TelemetryEvent::default());
        Ok(blueprints)
    }

    pub async fn delete_blueprint(&self, name: &str) -> AppResult<bool> {
        let trimmed = name.trim();
        if trimmed.is_empty() {
            return Err(AppError::new(
                AppErrorKind::InvalidArgument,
                codes::INVALID_ARGUMENT,
            ));
        }
        // If delete_all is true, all local blueprint versions are deleted.
        let (base_name, delete_name, delete_all) =
            if let Some((blueprint_name, tag)) = split_blueprint_ref(trimmed) {
                let base = blueprint_name.to_string();
                if tag == "latest" {
                    let latest = self.get_latest_blueprint_build(blueprint_name).await?;
                    let version_tag = latest.version_tag.clone().ok_or_else(|| {
                        invalid_argument("latest blueprint build is missing version tag")
                    })?;
                    (base, format!("{}:{}", latest.name, version_tag), false)
                } else {
                    (base, trimmed.to_string(), false)
                }
            } else {
                (trimmed.to_string(), String::new(), true)
            };
        let running_jobs = self.jobs.list_running_jobs().await?;
        if running_jobs
            .iter()
            .any(|job| job.blueprint_name.as_deref() == Some(base_name.as_str()))
        {
            return Err(AppError::with_message(
                AppErrorKind::Conflict,
                codes::CONFLICT,
                format!(
                    "deleting blueprint '{trimmed}' is prohibited because it has running jobs; \
find jobs for '{base_name}' and cancel them with `job cancel`"
                ),
            ));
        }

        let mut tarball_paths: Vec<PathBuf> = Vec::new();
        if delete_all {
            // Enumerating tarballs for the blueprint
            let blueprints = self.projects.list_blueprints().await?;
            for blueprint in blueprints {
                if blueprint.name == base_name {
                    if let Some(version_tag) = blueprint.version_tag.as_deref() {
                        let blueprint_ref = format!("{}:{version_tag}", blueprint.name);
                        tarball_paths.push(tarball_path_for_blueprint_ref(
                            &self.tarballs_dir,
                            &blueprint_ref,
                        ));
                    }
                }
            }
        } else if !delete_name.is_empty() {
            if let Some(blueprint) = self.projects.get_blueprint_by_name(&delete_name).await? {
                tarball_paths.push(tarball_path_for_blueprint_record(
                    &self.tarballs_dir,
                    &blueprint,
                )?);
            }
        }

        for tarball_path in tarball_paths {
            match std::fs::remove_file(&tarball_path) {
                Ok(()) => {}
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => {
                    return Err(local_error(format!(
                        "failed to delete tarball {}: {err}",
                        tarball_path.display()
                    )));
                }
            }
        }

        let deleted = if delete_all {
            self.projects
                .delete_blueprints_by_base_name(&base_name)
                .await?
        } else {
            self.projects.delete_blueprint_by_name(&delete_name).await?
        };
        if deleted > 0 {
            self.telemetry.event(
                "project.deleted",
                TelemetryEvent {
                    project: Some(trimmed.to_string()),
                    ..TelemetryEvent::default()
                },
            );
        }
        Ok(deleted > 0)
    }

    pub async fn build_blueprint(
        &self,
        path: &str,
        package_git: bool,
    ) -> AppResult<BlueprintRecord> {
        let resolved = resolve_blueprint_path(self.local_fs.as_ref(), path).await?;
        let blueprint_root = discover_orbitfile_root(&resolved)?
            .ok_or_else(|| invalid_argument("blueprint build requires an Orbitfile"))?;
        let orbitfile_path = blueprint_root.join(ORBITFILE_NAME);
        let orbitfile_metadata = load_orbitfile_metadata(&orbitfile_path)?;
        let OrbitfileMetadata {
            name: blueprint_name,
            default_retrieve_path,
            submit_sbatch_script: raw_submit_sbatch_script,
            sync_include,
            sync_exclude,
        } = orbitfile_metadata;
        validate_blueprint_name(&blueprint_name)?;

        let tarballs_dir = std::fs::canonicalize(&self.tarballs_dir).map_err(|err| {
            local_error(format!(
                "failed to resolve tarballs directory {}: {err}",
                self.tarballs_dir.display()
            ))
        })?;
        if tarballs_dir.starts_with(&blueprint_root) {
            return Err(invalid_argument(format!(
                "tarballs_dir '{}' cannot be inside blueprint root '{}'",
                tarballs_dir.display(),
                blueprint_root.display()
            )));
        }

        let submit_sbatch_script = match raw_submit_sbatch_script.as_deref() {
            Some(value) => Some(resolve_orbitfile_sbatch_script(
                &blueprint_root,
                &blueprint_root,
                value,
            )?),
            None => None,
        };
        let sbatch_scripts = collect_sbatch_scripts(&blueprint_root)?;
        if submit_sbatch_script.is_none() && sbatch_scripts.is_empty() {
            return Err(invalid_argument(format!(
                "no .sbatch files found under '{}'; set [submit].sbatch_script or add a .sbatch file",
                blueprint_root.display()
            )));
        }

        let (_version_tag, blueprint_ref) =
            self.next_blueprint_version_tag(&blueprint_name).await?;
        let tarball_path = tarball_path_for_blueprint_ref(&self.tarballs_dir, &blueprint_ref);

        let template_config_json = templates::load_template_config_json(&orbitfile_path)?;

        let root_name = blueprint_root
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| invalid_argument("blueprint path has no directory name"))?;
        project_building::create_tarball(&blueprint_root, root_name, &tarball_path, package_git)?;
        let tarball_hash = project_building::blake3_file_hash(&tarball_path)?;

        let build = NewBlueprintBuild {
            name: blueprint_ref,
            tarball_hash,
            tarball_hash_function: TARBALL_HASH_FUNCTION_BLAKE3.to_string(),
            tool_version: env!("CARGO_PKG_VERSION").to_string(),
            template_config_json,
            submit_sbatch_script,
            sbatch_scripts,
            default_retrieve_path,
            sync_include,
            sync_exclude,
        };
        let blueprint = self.projects.upsert_blueprint_build(&build).await?;
        self.telemetry.event(
            "project.built",
            TelemetryEvent {
                project: Some(blueprint_name),
                ..TelemetryEvent::default()
            },
        );
        Ok(blueprint)
    }

    pub async fn delete_cluster(&self, name: &str, force: bool) -> AppResult<bool> {
        let trimmed = name.trim();
        if trimmed.is_empty() {
            return Err(AppError::new(
                AppErrorKind::InvalidArgument,
                codes::INVALID_ARGUMENT,
            ));
        }
        let Some(host) = self.clusters.get_by_name(trimmed).await? else {
            return Err(AppError::new(
                AppErrorKind::InvalidArgument,
                codes::NOT_FOUND,
            ));
        };
        if !force {
            let running_jobs = self.jobs.list_running_jobs().await?;
            if running_jobs.iter().any(|job| job.name == trimmed) {
                return Err(AppError::with_message(
                    AppErrorKind::Conflict,
                    codes::CONFLICT,
                    format!(
                        "deleting cluster '{trimmed}' is prohibited because it has running jobs; \
cancel them with `job cancel` or pass --force"
                    ),
                ));
            }
        }
        let deleted = self.clusters.delete_by_name(trimmed).await?;
        //TODO: can this even happen, or this is over-checking?
        if deleted == 0 {
            return Err(AppError::new(
                AppErrorKind::InvalidArgument,
                codes::NOT_FOUND,
            ));
        }
        if host.is_default {
            self.promote_last_added_cluster_to_default().await?;
        }
        let _ = self.remote_exec.remove_session(trimmed).await;
        self.telemetry.event(
            "cluster.deleted",
            TelemetryEvent {
                cluster: Some(trimmed.to_string()),
                ..TelemetryEvent::default()
            },
        );
        Ok(true)
    }

    async fn promote_last_added_cluster_to_default(&self) -> AppResult<()> {
        let mut remaining_hosts = self.clusters.list_hosts(None).await?;
        let Some(last_added) = remaining_hosts.pop() else {
            return Ok(());
        };
        if last_added.is_default {
            return Ok(());
        }
        let replacement = host_record_to_new_host(&last_added, true);
        self.clusters.upsert_host(&replacement).await?;
        Ok(())
    }

    pub async fn resolve_home_dir(
        &self,
        input: ResolveHomeDirInput,
        stream: &dyn StreamOutputPort,
        mfa: &mut dyn MfaPort,
    ) -> AppResult<()> {
        let addr = self
            .network
            .resolve_host_addr(&input.address, input.port)
            .await?;
        let config = build_ssh_config(
            input.session_name,
            input.username,
            &input.address,
            addr,
            input.identity_path,
        );

        self.remote_exec
            .ensure_connected(&config, stream, mfa)
            .await?;

        let home = fetch_remote_home_dir(self.remote_exec.as_ref(), &config).await?;
        if !stream
            .send(StreamEvent {
                event: Some(stream_event::Event::Stdout(home.into_bytes())),
            })
            .await
            .is_ok()
        {
            return Ok(());
        }
        let _ = stream
            .send(StreamEvent {
                event: Some(stream_event::Event::ExitCode(0)),
            })
            .await;
        Ok(())
    }

    pub async fn resolve_scratch_directories(
        &self,
        input: ResolveScratchDirectoriesInput,
        stream: &dyn StreamOutputPort,
        mfa: &mut dyn MfaPort,
    ) -> AppResult<Vec<String>> {
        let addr = self
            .network
            .resolve_host_addr(&input.address, input.port)
            .await?;
        let config = build_ssh_config(
            input.session_name,
            input.username.clone(),
            &input.address,
            addr,
            input.identity_path,
        );

        self.remote_exec
            .ensure_connected(&config, stream, mfa)
            .await?;

        let home_dir = fetch_remote_home_dir(self.remote_exec.as_ref(), &config).await?;
        let username = fetch_remote_username(self.remote_exec.as_ref(), &config)
            .await
            .unwrap_or_else(|_| input.username.trim().to_string());
        let raw_candidates = vec![
            "~/scratch".to_string(),
            format!("/scratch/{username}"),
            format!("/localscratch/{username}"),
        ];
        tracing::debug!(
            "scratch discovery start session={} username={} candidates={:?}",
            config.session_name.as_deref().unwrap_or("<none>"),
            username,
            raw_candidates
        );

        let mut found = Vec::new();
        for raw in raw_candidates {
            let resolved = resolve_default_scratch_directory(Some(raw), &home_dir)?;
            let Some(normalized) = normalize_default_scratch_directory(resolved)? else {
                continue;
            };
            match validate_scratch_directory_access(self.remote_exec.as_ref(), &config, &normalized)
                .await
            {
                Ok(real_path) => {
                    tracing::debug!(
                        "scratch discovery accepted candidate={} resolved={}",
                        normalized,
                        real_path
                    );
                    if !found.contains(&normalized) {
                        found.push(normalized);
                    }
                }
                Err(err) => {
                    if err.kind() != AppErrorKind::InvalidArgument {
                        return Err(err);
                    }
                    tracing::debug!(
                        "scratch discovery rejected candidate={} reason={}",
                        normalized,
                        err.message()
                    );
                }
            }
        }
        tracing::debug!(
            "scratch discovery done session={} found={:?}",
            config.session_name.as_deref().unwrap_or("<none>"),
            found
        );
        Ok(found)
    }

    pub async fn validate_scratch_directory(
        &self,
        input: ValidateScratchDirectoryInput,
        stream: &dyn StreamOutputPort,
        mfa: &mut dyn MfaPort,
    ) -> AppResult<String> {
        let addr = self
            .network
            .resolve_host_addr(&input.address, input.port)
            .await?;
        let config = build_ssh_config(
            input.session_name,
            input.username,
            &input.address,
            addr,
            input.identity_path,
        );

        self.remote_exec
            .ensure_connected(&config, stream, mfa)
            .await?;

        let home_dir = fetch_remote_home_dir(self.remote_exec.as_ref(), &config).await?;
        let resolved = resolve_default_scratch_directory(Some(input.directory), &home_dir)?;
        let normalized = normalize_default_scratch_directory(resolved)?
            .ok_or_else(|| invalid_argument("scratch directory cannot be empty"))?;
        let real_path =
            validate_scratch_directory_access(self.remote_exec.as_ref(), &config, &normalized)
                .await?;
        tracing::info!(
            "scratch directory validated input={} resolved={}",
            normalized,
            real_path
        );
        Ok(normalized)
    }

    pub async fn validate_default_base_path(
        &self,
        input: ValidateDefaultBasePathInput,
        stream: &dyn StreamOutputPort,
        mfa: &mut dyn MfaPort,
    ) -> AppResult<String> {
        let addr = self
            .network
            .resolve_host_addr(&input.address, input.port)
            .await?;
        let config = build_ssh_config(
            input.session_name,
            input.username,
            &input.address,
            addr,
            input.identity_path,
        );

        self.remote_exec
            .ensure_connected(&config, stream, mfa)
            .await?;

        let home_dir = fetch_remote_home_dir(self.remote_exec.as_ref(), &config).await?;
        let resolved = resolve_default_base_path(Some(input.base_path), &home_dir)?;
        let normalized = normalize_default_base_path(resolved)?
            .ok_or_else(|| invalid_argument("default base path cannot be empty"))?;
        validate_and_prepare_default_base_path(self.remote_exec.as_ref(), &config, &normalized)
            .await?;
        tracing::info!("default base path validated resolved={}", normalized);
        Ok(normalized)
    }

    pub async fn ls(
        &self,
        input: LsInput,
        stream: &dyn StreamOutputPort,
        mfa: &mut dyn MfaPort,
    ) -> AppResult<()> {
        let (name, list_path) = if let Some(job_id) = input.job_id {
            let job = match self.jobs.get_job_by_job_id(job_id).await? {
                Some(v) => v,
                None => {
                    return Err(AppError::new(
                        AppErrorKind::InvalidArgument,
                        codes::NOT_FOUND,
                    ));
                }
            };
            if !input.name.is_empty() && input.name != job.name {
                return Err(AppError::new(
                    AppErrorKind::InvalidArgument,
                    codes::NOT_FOUND,
                ));
            }
            let list_path = match input.path {
                Some(v) => {
                    if v.is_empty() {
                        return Err(AppError::new(
                            AppErrorKind::InvalidArgument,
                            codes::INVALID_ARGUMENT,
                        ));
                    }
                    if PathBuf::from(&v).is_absolute() {
                        remote_path::normalize_path(v)
                            .to_string_lossy()
                            .into_owned()
                    } else {
                        remote_path::resolve_relative(&job.remote_path, v)
                            .to_string_lossy()
                            .into_owned()
                    }
                }
                None => remote_path::normalize_path(&job.remote_path)
                    .to_string_lossy()
                    .into_owned(),
            };
            (job.name, list_path)
        } else {
            if input.name.is_empty() {
                return Err(AppError::new(
                    AppErrorKind::InvalidArgument,
                    codes::INVALID_ARGUMENT,
                ));
            }
            let list_path = match input.path {
                Some(v) => {
                    if v.is_empty() {
                        return Err(AppError::new(
                            AppErrorKind::InvalidArgument,
                            codes::INVALID_ARGUMENT,
                        ));
                    }
                    if PathBuf::from(&v).is_absolute() {
                        remote_path::normalize_path(v)
                            .to_string_lossy()
                            .into_owned()
                    } else {
                        let default_base_path = self.get_default_base_path(&input.name).await?;
                        let base_path = PathBuf::from(default_base_path);
                        remote_path::resolve_relative(base_path, v)
                            .to_string_lossy()
                            .into_owned()
                    }
                }
                None => remote_path::normalize_path(self.get_default_base_path(&input.name).await?)
                    .to_string_lossy()
                    .into_owned(),
            };
            (input.name, list_path)
        };

        let command = format!("ls -- {}", shell::sh_escape(&list_path));
        self.run_command(&name, command, stream, mfa).await
    }

    pub async fn retrieve_job(
        &self,
        input: RetrieveJobInput,
        stream: &dyn StreamOutputPort,
        mfa: &mut dyn MfaPort,
    ) -> AppResult<()> {
        let local_path = match input.local_path {
            Some(v) if !v.trim().is_empty() => v,
            _ => {
                self.telemetry.event(
                    "job.retrieve.failed",
                    TelemetryEvent {
                        job_id: Some(input.job_id),
                        ..TelemetryEvent::default()
                    },
                );
                return Err(AppError::new(
                    AppErrorKind::InvalidArgument,
                    codes::INVALID_ARGUMENT,
                ));
            }
        };

        let job = match self.jobs.get_job_by_job_id(input.job_id).await? {
            Some(v) => v,
            None => {
                let _ = stream
                    .send(StreamEvent {
                        event: Some(stream_event::Event::Error(codes::NOT_FOUND.to_string())),
                    })
                    .await;
                self.telemetry.event(
                    "job.retrieve.failed",
                    TelemetryEvent {
                        job_id: Some(input.job_id),
                        ..TelemetryEvent::default()
                    },
                );
                return Ok(());
            }
        };
        let telemetry_base = TelemetryEvent {
            project: job.blueprint_name.clone(),
            cluster: Some(job.name.clone()),
            job_id: Some(input.job_id),
            remote_path: Some(job.remote_path.clone()),
            ..TelemetryEvent::default()
        };
        self.telemetry
            .event("job.retrieve.started", telemetry_base.clone());

        let requested_path = if input.path.trim().is_empty() {
            match job
                .default_retrieve_path
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
            {
                Some(value) => value.to_string(),
                None => {
                    self.telemetry
                        .event("job.retrieve.failed", telemetry_base.clone());
                    return Err(AppError::with_message(
                        AppErrorKind::InvalidArgument,
                        codes::INVALID_ARGUMENT,
                        format!(
                            "No default retrieve path set for job {}; pass a path or define [retrieve].default_path in Orbitfile at run time.",
                            input.job_id
                        ),
                    ));
                }
            }
        } else {
            input.path.clone()
        };

        if !job.is_completed && !input.force {
            let message = format!(
                "Job {} is not completed; use --force to retrieve anyway.\n",
                input.job_id
            );
            let _ = stream
                .send(StreamEvent {
                    event: Some(stream_event::Event::Stderr(message.into_bytes())),
                })
                .await;
            let _ = stream
                .send(StreamEvent {
                    event: Some(stream_event::Event::Error(codes::CONFLICT.to_string())),
                })
                .await;
            self.telemetry
                .event("job.retrieve.failed", telemetry_base.clone());
            return Ok(());
        }
        if !input.force {
            if let Some(state) = job.terminal_state.as_deref() {
                if !state.eq_ignore_ascii_case("COMPLETED") {
                    let message = format!(
                        "Job {} did not complete successfully; use --force to retrieve anyway.\n",
                        input.job_id
                    );
                    let _ = stream
                        .send(StreamEvent {
                            event: Some(stream_event::Event::Stderr(message.into_bytes())),
                        })
                        .await;
                    let _ = stream
                        .send(StreamEvent {
                            event: Some(stream_event::Event::Error(codes::CONFLICT.to_string())),
                        })
                        .await;
                    self.telemetry
                        .event("job.retrieve.failed", telemetry_base.clone());
                    return Ok(());
                }
            }
        }

        let config = match self.config_for_host_name(&job.name).await {
            Ok(value) => value,
            Err(err) => {
                let _ = stream
                    .send(StreamEvent {
                        event: Some(stream_event::Event::Error(err.code().to_string())),
                    })
                    .await;
                self.telemetry
                    .event("job.retrieve.failed", telemetry_base.clone());
                return Ok(());
            }
        };

        if let Err(err) = self
            .remote_exec
            .ensure_connected(&config, stream, mfa)
            .await
        {
            let _ = stream
                .send(StreamEvent {
                    event: Some(stream_event::Event::Error(err.code().to_string())),
                })
                .await;
            self.telemetry
                .event("job.retrieve.failed", telemetry_base.clone());
            return Ok(());
        }

        let path_is_absolute = PathBuf::from(&requested_path).is_absolute();
        let remote_path = if path_is_absolute {
            remote_path::normalize_path(&requested_path)
                .to_string_lossy()
                .into_owned()
        } else {
            remote_path::resolve_relative(&job.remote_path, &requested_path)
                .to_string_lossy()
                .into_owned()
        };

        let mut local_base = PathBuf::from(local_path);
        if !local_base.is_absolute() {
            match self.local_fs.current_dir().await {
                Ok(cwd) => local_base = cwd.join(local_base),
                Err(err) => {
                    let _ = stream
                        .send(StreamEvent {
                            event: Some(stream_event::Event::Error(codes::LOCAL_ERROR.to_string())),
                        })
                        .await;
                    tracing::debug!("could not resolve local destination: {err}");
                    self.telemetry
                        .event("job.retrieve.failed", telemetry_base.clone());
                    return Ok(());
                }
            }
        }

        let local_target = match resolve_retrieve_local_target(
            &requested_path,
            &remote_path,
            &local_base,
            path_is_absolute,
        ) {
            Some(target) => target,
            None => {
                let _ = stream
                    .send(StreamEvent {
                        event: Some(stream_event::Event::Error(
                            codes::INVALID_ARGUMENT.to_string(),
                        )),
                    })
                    .await;
                self.telemetry
                    .event("job.retrieve.failed", telemetry_base.clone());
                return Ok(());
            }
        };

        match self
            .file_sync
            .retrieve_path(&config, &remote_path, &local_target, input.overwrite)
            .await
        {
            Ok(()) => {
                let _ = stream
                    .send(StreamEvent {
                        event: Some(stream_event::Event::ExitCode(0)),
                    })
                    .await;
                self.telemetry
                    .event("job.retrieve.succeeded", telemetry_base.clone());
            }
            Err(err) if err.code() == codes::NOT_FOUND => {
                let _ = stream
                    .send(StreamEvent {
                        event: Some(stream_event::Event::Stderr(
                            b"No such file or directory\n".to_vec(),
                        )),
                    })
                    .await;
                let _ = stream
                    .send(StreamEvent {
                        event: Some(stream_event::Event::Error(codes::NOT_FOUND.to_string())),
                    })
                    .await;
                self.telemetry
                    .event("job.retrieve.failed", telemetry_base.clone());
            }
            Err(err) if err.code() == codes::CONFLICT => {
                let message = format!("{}; use --overwrite to replace it.\n", err.message());
                let _ = stream
                    .send(StreamEvent {
                        event: Some(stream_event::Event::Stderr(message.into_bytes())),
                    })
                    .await;
                let _ = stream
                    .send(StreamEvent {
                        event: Some(stream_event::Event::Error(codes::CONFLICT.to_string())),
                    })
                    .await;
                self.telemetry
                    .event("job.retrieve.failed", telemetry_base.clone());
            }
            Err(_) => {
                let _ = stream
                    .send(StreamEvent {
                        event: Some(stream_event::Event::Error(codes::REMOTE_ERROR.to_string())),
                    })
                    .await;
                self.telemetry
                    .event("job.retrieve.failed", telemetry_base.clone());
            }
        }

        Ok(())
    }

    pub async fn job_logs(
        &self,
        input: JobLogsInput,
        stream: &dyn StreamOutputPort,
        mfa: &mut dyn MfaPort,
    ) -> AppResult<()> {
        let job = match self.jobs.get_job_by_job_id(input.job_id).await? {
            Some(v) => v,
            None => {
                let message = format!(
                    "job {} does not exist; you can list all job with 'orbit job list'\n",
                    input.job_id
                );
                let _ = stream
                    .send(StreamEvent {
                        event: Some(stream_event::Event::Stderr(message.into_bytes())),
                    })
                    .await;
                let _ = stream
                    .send(StreamEvent {
                        event: Some(stream_event::Event::Error(codes::NOT_FOUND.to_string())),
                    })
                    .await;
                return Ok(());
            }
        };

        let log_path = if input.stderr {
            match job.stderr_path.clone() {
                Some(path) if !path.trim().is_empty() => path,
                _ => {
                    let message = format!(
                        "stderr log file is not configured for job {}\n",
                        input.job_id
                    );
                    let _ = stream
                        .send(StreamEvent {
                            event: Some(stream_event::Event::Stderr(message.into_bytes())),
                        })
                        .await;
                    let _ = stream
                        .send(StreamEvent {
                            event: Some(stream_event::Event::Error(
                                codes::INVALID_ARGUMENT.to_string(),
                            )),
                        })
                        .await;
                    return Ok(());
                }
            }
        } else {
            if job.stdout_path.trim().is_empty() {
                let message = format!(
                    "stdout log file is not configured for job {}\n",
                    input.job_id
                );
                let _ = stream
                    .send(StreamEvent {
                        event: Some(stream_event::Event::Stderr(message.into_bytes())),
                    })
                    .await;
                let _ = stream
                    .send(StreamEvent {
                        event: Some(stream_event::Event::Error(codes::NOT_FOUND.to_string())),
                    })
                    .await;
                return Ok(());
            }
            job.stdout_path.clone()
        };

        let config = match self.config_for_host_name(&job.name).await {
            Ok(value) => value,
            Err(err) => {
                let _ = stream
                    .send(StreamEvent {
                        event: Some(stream_event::Event::Error(err.code().to_string())),
                    })
                    .await;
                return Ok(());
            }
        };

        if let Err(err) = self
            .remote_exec
            .ensure_connected(&config, stream, mfa)
            .await
        {
            let _ = stream
                .send(StreamEvent {
                    event: Some(stream_event::Event::Error(err.code().to_string())),
                })
                .await;
            return Ok(());
        }

        let escaped = shell::sh_escape(&log_path);
        let test_cmd = format!("test -f {}", escaped);
        match self.remote_exec.exec_capture(&config, &test_cmd).await {
            Ok(result) if result.exit_code == 0 => {}
            Ok(_) => {
                let message = if input.stderr {
                    format!("specified error file {} wasn't found\n", log_path)
                } else {
                    format!("stdout log file does not exist at {}\n", log_path)
                };
                let _ = stream
                    .send(StreamEvent {
                        event: Some(stream_event::Event::Stderr(message.into_bytes())),
                    })
                    .await;
                let _ = stream
                    .send(StreamEvent {
                        event: Some(stream_event::Event::Error(codes::NOT_FOUND.to_string())),
                    })
                    .await;
                return Ok(());
            }
            Err(_) => {
                let _ = stream
                    .send(StreamEvent {
                        event: Some(stream_event::Event::Error(codes::REMOTE_ERROR.to_string())),
                    })
                    .await;
                return Ok(());
            }
        }

        let command = format!("cat -- {}", escaped);
        if let Err(_) = self
            .remote_exec
            .exec_stream(&config, &command, stream, mfa)
            .await
        {
            let _ = stream
                .send(StreamEvent {
                    event: Some(stream_event::Event::Error(codes::REMOTE_ERROR.to_string())),
                })
                .await;
        }
        Ok(())
    }

    pub async fn cancel_job(
        &self,
        input: CancelJobInput,
        stream: &dyn StreamOutputPort,
        mfa: &mut dyn MfaPort,
    ) -> AppResult<()> {
        let job = match self.jobs.get_job_by_job_id(input.job_id).await? {
            Some(v) => v,
            None => {
                let message = format!(
                    "job {} does not exist; you can list all job with 'orbit job list'\n",
                    input.job_id
                );
                let _ = stream
                    .send(StreamEvent {
                        event: Some(stream_event::Event::Stderr(message.into_bytes())),
                    })
                    .await;
                let _ = stream
                    .send(StreamEvent {
                        event: Some(stream_event::Event::Error(codes::NOT_FOUND.to_string())),
                    })
                    .await;
                return Ok(());
            }
        };

        if job.is_completed {
            let message = format!(
                "job {} is already completed; cancel is not possible\n",
                input.job_id
            );
            let _ = stream
                .send(StreamEvent {
                    event: Some(stream_event::Event::Stderr(message.into_bytes())),
                })
                .await;
            let _ = stream
                .send(StreamEvent {
                    event: Some(stream_event::Event::Error(codes::CONFLICT.to_string())),
                })
                .await;
            return Ok(());
        }

        let Some(scheduler_id) = job.scheduler_id else {
            let message = format!(
                "job {} has no scheduler id; cancel is not possible\n",
                input.job_id
            );
            let _ = stream
                .send(StreamEvent {
                    event: Some(stream_event::Event::Stderr(message.into_bytes())),
                })
                .await;
            let _ = stream
                .send(StreamEvent {
                    event: Some(stream_event::Event::Error(
                        codes::INTERNAL_ERROR.to_string(),
                    )),
                })
                .await;
            return Ok(());
        };

        let config = match self.config_for_host_name(&job.name).await {
            Ok(value) => value,
            Err(err) => {
                let _ = stream
                    .send(StreamEvent {
                        event: Some(stream_event::Event::Error(err.code().to_string())),
                    })
                    .await;
                return Ok(());
            }
        };

        if let Err(err) = self
            .remote_exec
            .ensure_connected(&config, stream, mfa)
            .await
        {
            let _ = stream
                .send(StreamEvent {
                    event: Some(stream_event::Event::Error(err.code().to_string())),
                })
                .await;
            return Ok(());
        }

        let command = format!("scancel {}", shell::sh_escape(&scheduler_id.to_string()));
        let capture = match self.remote_exec.exec_capture(&config, &command).await {
            Ok(v) => v,
            Err(_) => {
                let _ = stream
                    .send(StreamEvent {
                        event: Some(stream_event::Event::Error(codes::REMOTE_ERROR.to_string())),
                    })
                    .await;
                return Ok(());
            }
        };

        if capture.exit_code != 0 {
            let err_text = String::from_utf8_lossy(&capture.stderr);
            let detail = if err_text.trim().is_empty() {
                format!("exit code {}", capture.exit_code)
            } else {
                err_text.trim().to_string()
            };
            let message = format!("failed to cancel job {}: {}\n", input.job_id, detail);
            let _ = stream
                .send(StreamEvent {
                    event: Some(stream_event::Event::Stderr(message.into_bytes())),
                })
                .await;
            let _ = stream
                .send(StreamEvent {
                    event: Some(stream_event::Event::Error(codes::REMOTE_ERROR.to_string())),
                })
                .await;
            return Ok(());
        }

        if let Err(_) = self
            .jobs
            .mark_job_completed(input.job_id, Some("CANCELED"))
            .await
        {
            let _ = stream
                .send(StreamEvent {
                    event: Some(stream_event::Event::Error(
                        codes::INTERNAL_ERROR.to_string(),
                    )),
                })
                .await;
            return Ok(());
        }

        let message = format!("Job {} canceled.\n", input.job_id);
        let _ = stream
            .send(StreamEvent {
                event: Some(stream_event::Event::Stdout(message.into_bytes())),
            })
            .await;
        let _ = stream
            .send(StreamEvent {
                event: Some(stream_event::Event::ExitCode(0)),
            })
            .await;
        Ok(())
    }

    pub async fn cleanup_job(
        &self,
        input: CleanupJobInput,
        stream: &dyn StreamOutputPort,
        mfa: &mut dyn MfaPort,
    ) -> AppResult<()> {
        let job = match self.jobs.get_job_by_job_id(input.job_id).await? {
            Some(v) => v,
            None => {
                let message = format!(
                    "job {} does not exist; you can list all job with 'orbit job list'\n",
                    input.job_id
                );
                let _ = stream
                    .send(StreamEvent {
                        event: Some(stream_event::Event::Stderr(message.into_bytes())),
                    })
                    .await;
                let _ = stream
                    .send(StreamEvent {
                        event: Some(stream_event::Event::Error(codes::NOT_FOUND.to_string())),
                    })
                    .await;
                self.telemetry.event(
                    "job.cleanup.failed",
                    TelemetryEvent {
                        job_id: Some(input.job_id),
                        ..TelemetryEvent::default()
                    },
                );
                return Ok(());
            }
        };

        let telemetry_base = TelemetryEvent {
            project: job.blueprint_name.clone(),
            cluster: Some(job.name.clone()),
            job_id: Some(input.job_id),
            remote_path: Some(job.remote_path.clone()),
            ..TelemetryEvent::default()
        };
        self.telemetry
            .event("job.cleanup.started", telemetry_base.clone());

        if is_unsafe_cleanup_path(&job.remote_path) {
            let message = format!(
                "job {} has an unsafe remote path '{}' and cannot be cleaned up\n",
                input.job_id, job.remote_path
            );
            let _ = stream
                .send(StreamEvent {
                    event: Some(stream_event::Event::Stderr(message.into_bytes())),
                })
                .await;
            let _ = stream
                .send(StreamEvent {
                    event: Some(stream_event::Event::Error(
                        codes::INVALID_ARGUMENT.to_string(),
                    )),
                })
                .await;
            self.telemetry
                .event("job.cleanup.failed", telemetry_base.clone());
            return Ok(());
        }

        if !job.is_completed && !input.force {
            let message = format!(
                "job {} is still running; pass --force to cancel and clean up\n",
                input.job_id
            );
            let _ = stream
                .send(StreamEvent {
                    event: Some(stream_event::Event::Stderr(message.into_bytes())),
                })
                .await;
            let _ = stream
                .send(StreamEvent {
                    event: Some(stream_event::Event::ExitCode(1)),
                })
                .await;
            self.telemetry
                .event("job.cleanup.failed", telemetry_base.clone());
            return Ok(());
        }

        let config = match self.config_for_host_name(&job.name).await {
            Ok(value) => value,
            Err(err) => {
                let _ = stream
                    .send(StreamEvent {
                        event: Some(stream_event::Event::Error(err.code().to_string())),
                    })
                    .await;
                return Ok(());
            }
        };

        if let Err(err) = self
            .remote_exec
            .ensure_connected(&config, stream, mfa)
            .await
        {
            let _ = stream
                .send(StreamEvent {
                    event: Some(stream_event::Event::Error(err.code().to_string())),
                })
                .await;
            return Ok(());
        }

        if !job.is_completed {
            let Some(scheduler_id) = job.scheduler_id else {
                let message = format!(
                    "job {} has no scheduler id; cleanup is not possible\n",
                    input.job_id
                );
                let _ = stream
                    .send(StreamEvent {
                        event: Some(stream_event::Event::Stderr(message.into_bytes())),
                    })
                    .await;
                let _ = stream
                    .send(StreamEvent {
                        event: Some(stream_event::Event::Error(
                            codes::INTERNAL_ERROR.to_string(),
                        )),
                    })
                    .await;
                self.telemetry
                    .event("job.cleanup.failed", telemetry_base.clone());
                return Ok(());
            };

            let command = format!("scancel {}", shell::sh_escape(&scheduler_id.to_string()));
            let capture = match self.remote_exec.exec_capture(&config, &command).await {
                Ok(v) => v,
                Err(_) => {
                    let _ = stream
                        .send(StreamEvent {
                            event: Some(stream_event::Event::Error(
                                codes::REMOTE_ERROR.to_string(),
                            )),
                        })
                        .await;
                    self.telemetry
                        .event("job.cleanup.failed", telemetry_base.clone());
                    return Ok(());
                }
            };

            if capture.exit_code != 0 {
                let err_text = String::from_utf8_lossy(&capture.stderr);
                if !is_invalid_job_id(&err_text) {
                    let detail = if err_text.trim().is_empty() {
                        format!("exit code {}", capture.exit_code)
                    } else {
                        err_text.trim().to_string()
                    };
                    let message = format!("failed to cancel job {}: {}\n", input.job_id, detail);
                    let _ = stream
                        .send(StreamEvent {
                            event: Some(stream_event::Event::Stderr(message.into_bytes())),
                        })
                        .await;
                    let _ = stream
                        .send(StreamEvent {
                            event: Some(stream_event::Event::Error(
                                codes::REMOTE_ERROR.to_string(),
                            )),
                        })
                        .await;
                    self.telemetry
                        .event("job.cleanup.failed", telemetry_base.clone());
                    return Ok(());
                }
            }

            let start = Instant::now();
            loop {
                let command = format!("squeue -j {scheduler_id} -h -o %T");
                match self.remote_exec.exec_capture(&config, &command).await {
                    Ok(result) => {
                        if result.exit_code != 0 {
                            let err_text = String::from_utf8_lossy(&result.stderr);
                            if is_invalid_job_id(&err_text) {
                                break;
                            }
                            let message = format!(
                                "failed while waiting for job {} to cancel: {}\n",
                                input.job_id,
                                err_text.trim()
                            );
                            let _ = stream
                                .send(StreamEvent {
                                    event: Some(stream_event::Event::Stderr(message.into_bytes())),
                                })
                                .await;
                            let _ = stream
                                .send(StreamEvent {
                                    event: Some(stream_event::Event::Error(
                                        codes::REMOTE_ERROR.to_string(),
                                    )),
                                })
                                .await;
                            self.telemetry
                                .event("job.cleanup.failed", telemetry_base.clone());
                            return Ok(());
                        }
                        let output = String::from_utf8_lossy(&result.stdout);
                        if let Some(state) = parse_squeue_state(&output) {
                            if slurm::slurm_state_is_terminal(&state) {
                                break;
                            }
                        } else {
                            break;
                        }
                    }
                    Err(_) => {
                        let _ = stream
                            .send(StreamEvent {
                                event: Some(stream_event::Event::Error(
                                    codes::REMOTE_ERROR.to_string(),
                                )),
                            })
                            .await;
                        self.telemetry
                            .event("job.cleanup.failed", telemetry_base.clone());
                        return Ok(());
                    }
                }

                if start.elapsed() >= CLEANUP_CANCEL_TIMEOUT {
                    let message = format!("timed out waiting for job {} to cancel\n", input.job_id);
                    let _ = stream
                        .send(StreamEvent {
                            event: Some(stream_event::Event::Stderr(message.into_bytes())),
                        })
                        .await;
                    let _ = stream
                        .send(StreamEvent {
                            event: Some(stream_event::Event::Error(codes::CONFLICT.to_string())),
                        })
                        .await;
                    self.telemetry
                        .event("job.cleanup.failed", telemetry_base.clone());
                    return Ok(());
                }
                sleep(CLEANUP_CANCEL_POLL_INTERVAL).await;
            }

            if let Err(_) = self
                .jobs
                .mark_job_completed(input.job_id, Some("CANCELED"))
                .await
            {
                let _ = stream
                    .send(StreamEvent {
                        event: Some(stream_event::Event::Error(
                            codes::INTERNAL_ERROR.to_string(),
                        )),
                    })
                    .await;
                self.telemetry
                    .event("job.cleanup.failed", telemetry_base.clone());
                return Ok(());
            }
        }

        let command = format!("rm -rf -- {}", shell::sh_escape(&job.remote_path));
        let capture = match self.remote_exec.exec_capture(&config, &command).await {
            Ok(v) => v,
            Err(_) => {
                let _ = stream
                    .send(StreamEvent {
                        event: Some(stream_event::Event::Error(codes::REMOTE_ERROR.to_string())),
                    })
                    .await;
                self.telemetry
                    .event("job.cleanup.failed", telemetry_base.clone());
                return Ok(());
            }
        };

        if capture.exit_code != 0 {
            let err_text = String::from_utf8_lossy(&capture.stderr);
            let detail = if err_text.trim().is_empty() {
                format!("exit code {}", capture.exit_code)
            } else {
                err_text.trim().to_string()
            };
            let message = format!(
                "failed to remove remote directory for job {}: {}\n",
                input.job_id, detail
            );
            let _ = stream
                .send(StreamEvent {
                    event: Some(stream_event::Event::Stderr(message.into_bytes())),
                })
                .await;
            let _ = stream
                .send(StreamEvent {
                    event: Some(stream_event::Event::Error(codes::REMOTE_ERROR.to_string())),
                })
                .await;
            self.telemetry
                .event("job.cleanup.failed", telemetry_base.clone());
            return Ok(());
        }

        if input.full {
            match self.jobs.delete_job_by_job_id(input.job_id).await {
                Ok(_) => {}
                Err(_) => {
                    let _ = stream
                        .send(StreamEvent {
                            event: Some(stream_event::Event::Error(
                                codes::INTERNAL_ERROR.to_string(),
                            )),
                        })
                        .await;
                    self.telemetry
                        .event("job.cleanup.failed", telemetry_base.clone());
                    return Ok(());
                }
            }
        }

        let message = if input.full {
            format!("Job {} cleaned up and deleted.\n", input.job_id)
        } else {
            format!("Job {} cleaned up.\n", input.job_id)
        };
        let _ = stream
            .send(StreamEvent {
                event: Some(stream_event::Event::Stdout(message.into_bytes())),
            })
            .await;
        let _ = stream
            .send(StreamEvent {
                event: Some(stream_event::Event::ExitCode(0)),
            })
            .await;
        self.telemetry
            .event("job.cleanup.succeeded", telemetry_base.clone());
        Ok(())
    }

    pub async fn run_job(
        &self,
        input: RunJobInput,
        stream: &dyn RunStreamOutputPort,
        mfa: &mut dyn MfaPort,
        cancel_rx: watch::Receiver<bool>,
    ) -> AppResult<()> {
        let submit_input = RunInput {
            local_path: input.local_path,
            remote_path: input.remote_path,
            name: input.name,
            sbatchscript: input.sbatchscript,
            filters: input.filters,
            new_directory: input.new_directory,
            blueprint_name: input.blueprint_name,
            blueprint_tag: None,
            default_retrieve_path: input.default_retrieve_path,
            template_values_json: input.template_values_json,
        };
        self.submit_inner(submit_input, stream, mfa, cancel_rx)
            .await
    }

    pub async fn run_blueprint(
        &self,
        input: RunBlueprintInput,
        stream: &dyn RunStreamOutputPort,
        mfa: &mut dyn MfaPort,
        cancel_rx: watch::Receiver<bool>,
    ) -> AppResult<()> {
        let submit_input = RunInput {
            local_path: String::new(),
            remote_path: input.remote_path,
            name: input.name,
            sbatchscript: input.sbatchscript,
            filters: input.filters,
            new_directory: input.new_directory,
            blueprint_name: Some(input.blueprint_name),
            blueprint_tag: Some(input.blueprint_tag),
            default_retrieve_path: input.default_retrieve_path,
            template_values_json: input.template_values_json,
        };
        self.submit_inner(submit_input, stream, mfa, cancel_rx)
            .await
    }

    async fn submit_inner(
        &self,
        input: RunInput,
        stream: &dyn RunStreamOutputPort,
        mfa: &mut dyn MfaPort,
        mut cancel_rx: watch::Receiver<bool>,
    ) -> AppResult<()> {
        // Because the majority of logic for blueprints and jobs are reused, both are handled through the single interface right now
        let input = input;
        let cluster_name = input.name.clone();
        let blueprint_name = input.blueprint_name.clone();
        let submit_span = tracing::info_span!(
            "job_submit",
            project = blueprint_name.as_deref(),
            cluster = %cluster_name,
            job_name = tracing::field::Empty
        );
        let _submit_guard = submit_span.enter();
        let host = match self.clusters.get_by_name(&input.name).await? {
            Some(v) => v,
            None => {
                self.telemetry.event(
                    "job.submit.failed",
                    TelemetryEvent {
                        cluster: Some(cluster_name.clone()),
                        project: blueprint_name.clone(),
                        ..TelemetryEvent::default()
                    },
                );
                return Err(AppError::new(
                    AppErrorKind::InvalidArgument,
                    codes::NOT_FOUND,
                ));
            }
        };
        let config = self.config_for_host(&host).await.map_err(|err| {
            AppError::with_message(AppErrorKind::Internal, err.code(), err.message())
        })?;
        let telemetry_base = TelemetryEvent {
            cluster: Some(cluster_name.clone()),
            project: blueprint_name.clone(),
            user: Some(host.username.clone()),
            ..TelemetryEvent::default()
        };

        if input.blueprint_tag.is_none() && input.local_path.trim().is_empty() {
            return Err(invalid_argument("local path is required"));
        }

        let mut sync_root = PathBuf::from(&input.local_path);
        let mut template_values_json = None;
        let mut _template_guard = None;
        let mut _tarball_guard: Option<TempDir> = None;
        if let Some(blueprint_tag) = input.blueprint_tag.as_deref() {
            let blueprint_name = input
                .blueprint_name
                .clone()
                .ok_or_else(|| invalid_argument("blueprint tag provided without blueprint name"))?;
            let blueprint = if blueprint_tag == "latest" {
                self.get_latest_blueprint_build(&blueprint_name).await?
            } else {
                let blueprint_ref = format!("{blueprint_name}:{blueprint_tag}");
                self.projects
                    .get_blueprint_by_name(&blueprint_ref)
                    .await?
                    .ok_or_else(|| AppError::new(AppErrorKind::InvalidArgument, codes::NOT_FOUND))?
            };
            let tarball_path = tarball_path_for_blueprint_record(&self.tarballs_dir, &blueprint)?;
            if !tarball_path.is_file() {
                return Err(AppError::with_message(
                    AppErrorKind::InvalidArgument,
                    codes::NOT_FOUND,
                    format!("blueprint tarball not found at {}", tarball_path.display()),
                ));
            }
            let temp_dir = TempDir::new()
                .map_err(|err| local_error(format!("failed to create temp dir: {err}")))?;
            project_building::unpack_tarball(&tarball_path, temp_dir.path())?;
            sync_root = project_building::resolve_extracted_root(temp_dir.path(), None)?;
            _tarball_guard = Some(temp_dir);
        }

        if let Some(prepared) = templates::prepare_template_submission(
            &sync_root,
            input.template_values_json.as_deref(),
        )? {
            sync_root = prepared.staging_root;
            template_values_json = Some(prepared.values_json);
            _template_guard = Some(prepared.temp_dir);
        }

        let is_blueprint_submission = input.blueprint_tag.is_some();
        let use_combination_reuse = input.remote_path.is_none() && !input.new_directory;
        let blueprint_name_for_submission =
            if is_blueprint_submission {
                Some(input.blueprint_name.as_deref().ok_or_else(|| {
                    invalid_argument("blueprint tag provided without blueprint name")
                })?)
            } else {
                None
            };

        if use_combination_reuse {
            let running_job_id = if is_blueprint_submission {
                self.jobs
                    .running_job_id_for_blueprint(
                        &input.name,
                        blueprint_name_for_submission
                            .expect("blueprint name must exist for blueprint run"),
                        template_values_json.as_deref(),
                    )
                    .await?
            } else {
                self.jobs
                    .running_job_id_for_local_path(
                        &input.name,
                        &input.local_path,
                        template_values_json.as_deref(),
                    )
                    .await?
            };

            if let Some(job_id) = running_job_id {
                let detail = if is_blueprint_submission {
                    format!(
                        "job {job_id} with blueprint '{blueprint_name}' on cluster '{}' and current template values is still running; cancel it first or run in a new directory with --new-directory or --remote-path",
                        input.name,
                        blueprint_name = blueprint_name_for_submission
                            .expect("blueprint name must exist for blueprint run")
                    )
                } else {
                    format!(
                        "job {job_id} with cluster '{}' + local path '{}' and current template values is still running; cancel it first or run in a new directory with --new-directory or --remote-path",
                        input.name, input.local_path
                    )
                };
                self.telemetry.event(
                    "job.submit.failed",
                    TelemetryEvent {
                        cluster: Some(cluster_name.clone()),
                        project: blueprint_name.clone(),
                        ..TelemetryEvent::default()
                    },
                );
                return Err(AppError::with_message(
                    AppErrorKind::AlreadyExists,
                    codes::CONFLICT,
                    detail,
                ));
            }
        }

        let reuse_remote_path = if use_combination_reuse {
            if is_blueprint_submission {
                self.jobs
                    .latest_remote_path_for_blueprint(
                        &input.name,
                        blueprint_name_for_submission
                            .expect("blueprint name must exist for blueprint run"),
                        template_values_json.as_deref(),
                    )
                    .await?
            } else {
                self.jobs
                    .latest_remote_path_for_local_path(
                        &input.name,
                        &input.local_path,
                        template_values_json.as_deref(),
                    )
                    .await?
            }
        } else {
            None
        };
        let (remote_path, allow_existing_remote_path) = match reuse_remote_path {
            Some(value) => (value, true),
            None => {
                let resolved = match input.remote_path.as_deref() {
                    Some(v) if PathBuf::from(v).is_absolute() => {
                        run_paths::resolve_run_remote_path(Some(v), v, "")?
                    }
                    other => {
                        let default_base_path = self.get_default_base_path(&input.name).await?;
                        let random_suffix = random::generate_run_directory_name();
                        run_paths::resolve_run_remote_path(
                            other,
                            &default_base_path,
                            &random_suffix,
                        )?
                    }
                };
                (resolved, false)
            }
        };
        let telemetry_context = TelemetryEvent {
            remote_path: Some(remote_path.clone()),
            ..telemetry_base.clone()
        };
        self.telemetry
            .event("job.submit.started", telemetry_context.clone());

        if let Some(job_id) = self
            .jobs
            .running_job_id_for_remote_path(&input.name, &remote_path)
            .await?
        {
            let detail = format!(
                "job {job_id} is still running in {remote_path}; cancel it first or run in a new directory with --new-directory or --remote-path"
            );
            self.telemetry
                .event("job.submit.failed", telemetry_context.clone());
            return Err(AppError::with_message(
                AppErrorKind::AlreadyExists,
                codes::CONFLICT,
                detail,
            ));
        }

        if stream
            .send(RunStreamEvent {
                event: Some(run_stream_event::Event::RunStatus(RunStatus {
                    name: input.name.clone(),
                    remote_path: remote_path.clone(),
                    phase: run_status::Phase::Resolved as i32,
                })),
            })
            .await
            .is_err()
        {
            self.telemetry
                .event("job.submit.failed", telemetry_context.clone());
            return Err(AppError::new(AppErrorKind::Cancelled, codes::CANCELED));
        }

        if *cancel_rx.borrow() {
            self.telemetry
                .event("job.submit.failed", telemetry_context.clone());
            return Err(AppError::new(AppErrorKind::Cancelled, codes::CANCELED));
        }

        if let Err(err) = self
            .remote_exec
            .ensure_connected_submit(&config, stream, mfa)
            .await
        {
            self.telemetry
                .event("job.submit.failed", telemetry_context.clone());
            return Err(AppError::with_message(
                AppErrorKind::Internal,
                err.code(),
                err.message(),
            ));
        }

        let remote_path_exists = self
            .remote_exec
            .directory_exists(&config, &remote_path)
            .await?;
        if remote_path_exists && !allow_existing_remote_path {
            self.telemetry
                .event("job.submit.failed", telemetry_context.clone());
            return Err(AppError::new(AppErrorKind::AlreadyExists, codes::CONFLICT));
        }

        if stream
            .send(RunStreamEvent {
                event: Some(run_stream_event::Event::RunStatus(RunStatus {
                    name: input.name.clone(),
                    remote_path: remote_path.clone(),
                    phase: run_status::Phase::TransferStart as i32,
                })),
            })
            .await
            .is_err()
        {
            self.telemetry
                .event("job.submit.failed", telemetry_context.clone());
            return Ok(());
        }

        let sync_options = SyncOptions {
            block_size: Some(1024 * 1024),
            parallelism: None,
            filters: input.filters.clone(),
        };
        let sync_result = tokio::select! {
            res = self.file_sync.sync_dir(
                &config,
                &sync_root,
                &remote_path,
                sync_options,
                stream,
                mfa,
            ) => res,
            _ = cancel_rx.changed() => {
                return Ok(());
            }
        };
        if let Err(_) = sync_result {
            let _ = stream
                .send(RunStreamEvent {
                    event: Some(run_stream_event::Event::RunResult(RunResult {
                        status: run_result::Status::Failed as i32,
                        job_id: None,
                        detail: codes::REMOTE_ERROR.to_string(),
                    })),
                })
                .await;
            self.telemetry
                .event("job.submit.failed", telemetry_context.clone());
            return Ok(());
        }

        if stream
            .send(RunStreamEvent {
                event: Some(run_stream_event::Event::RunStatus(RunStatus {
                    name: input.name.clone(),
                    remote_path: remote_path.clone(),
                    phase: run_status::Phase::TransferDone as i32,
                })),
            })
            .await
            .is_err()
        {
            self.telemetry
                .event("job.submit.failed", telemetry_context.clone());
            return Ok(());
        }
        if stream.is_closed() || *cancel_rx.borrow() {
            self.telemetry
                .event("job.submit.failed", telemetry_context.clone());
            return Ok(());
        }

        let remote_sbatch_script_path =
            run_paths::resolve_remote_sbatch_path(&remote_path, &input.sbatchscript);
        let sbatch_command =
            slurm::path_to_sbatch_command(&remote_sbatch_script_path, Some(&remote_path));
        let exec_result = tokio::select! {
            res = self.remote_exec.exec_capture(&config, &sbatch_command) => res,
            _ = cancel_rx.changed() => {
                return Ok(());
            }
        };
        let capture = match exec_result {
            Ok(v) => v,
            Err(_) => {
                let _ = stream
                    .send(RunStreamEvent {
                        event: Some(run_stream_event::Event::RunResult(RunResult {
                            status: run_result::Status::Failed as i32,
                            job_id: None,
                            detail: codes::REMOTE_ERROR.to_string(),
                        })),
                    })
                    .await;
                self.telemetry
                    .event("job.submit.failed", telemetry_context.clone());
                return Ok(());
            }
        };

        if capture.exit_code != 0 {
            let err_message = String::from_utf8_lossy(&capture.stderr);
            let out_message = String::from_utf8_lossy(&capture.stdout);
            let detail = if err_message.trim().is_empty() {
                if out_message.trim().is_empty() {
                    "no error output from sbatch".to_string()
                } else {
                    out_message.trim().to_string()
                }
            } else {
                err_message.trim().to_string()
            };
            let run_detail = format!("{}\n{detail}", codes::SBATCH_SUBMIT_FAILED);
            let _ = stream
                .send(RunStreamEvent {
                    event: Some(run_stream_event::Event::RunResult(RunResult {
                        status: run_result::Status::Failed as i32,
                        job_id: None,
                        detail: run_detail,
                    })),
                })
                .await;
            tracing::debug!("sbatch failed: {detail}");
            self.telemetry
                .event("job.submit.failed", telemetry_context.clone());
            return Ok(());
        }

        let out_string = String::from_utf8_lossy(&capture.stdout);
        let Some(scheduler_id) = slurm::parse_job_id(&out_string) else {
            let _ = stream
                .send(RunStreamEvent {
                    event: Some(run_stream_event::Event::RunResult(RunResult {
                        status: run_result::Status::Failed as i32,
                        job_id: None,
                        detail: codes::REMOTE_ERROR.to_string(),
                    })),
                })
                .await;
            self.telemetry
                .event("job.submit.failed", telemetry_context.clone());
            return Ok(());
        };

        let sbatch_path = {
            let sbatch_path = PathBuf::from(&input.sbatchscript);
            if sbatch_path.is_absolute() {
                sbatch_path
            } else {
                sync_root.join(&input.sbatchscript)
            }
        };

        let templates = match self.local_fs.read_to_string(&sbatch_path).await {
            Ok(contents) => sbatch::parse_sbatch_log_templates(&contents),
            Err(err) => {
                tracing::warn!(
                    "submit log parse failed name={} sbatchscript={} error={}",
                    input.name,
                    sbatch_path.to_string_lossy(),
                    err
                );
                sbatch::SbatchLogTemplates {
                    stdout: None,
                    stderr: None,
                    job_name: None,
                }
            }
        };

        let sbatch::SbatchLogTemplates {
            stdout,
            stderr,
            job_name,
        } = templates;
        let default_job_name = sbatch_path
            .file_name()
            .and_then(|name| name.to_str())
            .filter(|name| !name.trim().is_empty())
            .unwrap_or("job");
        let job_name = job_name.unwrap_or_else(|| default_job_name.to_string());
        submit_span.record("job_name", &job_name.as_str());
        let stdout_template = stdout.unwrap_or_else(|| sbatch::DEFAULT_STDOUT_TEMPLATE.to_string());
        let stdout_path = sbatch::resolve_log_path(
            &stdout_template,
            &remote_path,
            scheduler_id,
            Some(job_name.as_str()),
            Some(host.username.as_str()),
        );
        let stderr_path = stderr
            .and_then(|value| {
                let trimmed = value.trim();
                if trimmed.is_empty() {
                    None
                } else {
                    Some(trimmed.to_string())
                }
            })
            .map(|template| {
                sbatch::resolve_log_path(
                    &template,
                    &remote_path,
                    scheduler_id,
                    Some(job_name.as_str()),
                    Some(host.username.as_str()),
                )
            });

        let job = NewJob {
            scheduler_id: Some(scheduler_id),
            host_id: host.id,
            local_path: if is_blueprint_submission {
                None
            } else {
                Some(input.local_path.clone())
            },
            remote_path: remote_path.clone(),
            stdout_path,
            stderr_path,
            blueprint_name: input.blueprint_name.clone(),
            default_retrieve_path: input.default_retrieve_path.clone(),
            template_values: template_values_json,
        };

        match self.jobs.insert_job(&job).await {
            Ok(job_id) => {
                let _ = stream
                    .send(RunStreamEvent {
                        event: Some(run_stream_event::Event::RunResult(RunResult {
                            status: run_result::Status::Submitted as i32,
                            job_id: Some(job_id),
                            detail: String::new(),
                        })),
                    })
                    .await;
                let telemetry_success = TelemetryEvent {
                    job_id: Some(job_id),
                    job_name: Some(job_name.clone()),
                    ..telemetry_context.clone()
                };
                self.telemetry
                    .event("job.submit.succeeded", telemetry_success);
            }
            Err(_) => {
                let _ = stream
                    .send(RunStreamEvent {
                        event: Some(run_stream_event::Event::RunResult(RunResult {
                            status: run_result::Status::Failed as i32,
                            job_id: None,
                            detail: codes::INTERNAL_ERROR.to_string(),
                        })),
                    })
                    .await;
                let telemetry_failed = TelemetryEvent {
                    job_name: Some(job_name.clone()),
                    ..telemetry_context.clone()
                };
                self.telemetry.event("job.submit.failed", telemetry_failed);
            }
        }

        Ok(())
    }

    pub async fn add_cluster(
        &self,
        input: AddClusterInput,
        stream: &dyn StreamOutputPort,
        mfa: &mut dyn MfaPort,
        default_selection: &mut mpsc::Receiver<proto::AddClusterDefaultSelection>,
    ) -> AppResult<()> {
        let cluster_name = input.name.clone();
        let user_name = input.username.clone();
        let result = self
            .cluster_upsert(
                ClusterUpsertInput {
                    name: input.name,
                    username: input.username,
                    address: input.address,
                    port: input.port,
                    identity_path: input.identity_path,
                    default_base_path: input.default_base_path,
                    default_scratch_directory: input.default_scratch_directory,
                    is_default: false,
                    emit_progress: true,
                },
                stream,
                mfa,
                Some(default_selection),
            )
            .await;
        if result.is_ok() {
            self.telemetry.event(
                "cluster.added",
                TelemetryEvent {
                    cluster: Some(cluster_name),
                    user: Some(user_name),
                    ..TelemetryEvent::default()
                },
            );
        }
        result
    }

    pub async fn set_cluster(
        &self,
        input: SetClusterInput,
        stream: &dyn StreamOutputPort,
        mfa: &mut dyn MfaPort,
    ) -> AppResult<()> {
        if input.name.trim().is_empty() {
            return Err(AppError::new(
                AppErrorKind::InvalidArgument,
                codes::INVALID_ARGUMENT,
            ));
        }
        let existing = match self.clusters.get_by_name(&input.name).await? {
            Some(v) => v,
            None => {
                return Err(AppError::new(
                    AppErrorKind::InvalidArgument,
                    codes::NOT_FOUND,
                ));
            }
        };

        let address = match input.address {
            Some(v) => v,
            None => existing.address.clone(),
        };
        let event_user = input.username.clone();
        let username = match input.username.as_deref() {
            Some(value) => {
                let trimmed = value.trim();
                if trimmed.is_empty() {
                    return Err(AppError::new(
                        AppErrorKind::InvalidArgument,
                        codes::INVALID_ARGUMENT,
                    ));
                }
                trimmed.to_string()
            }
            None => existing.username.clone(),
        };
        let identity_path = input
            .identity_path
            .or_else(|| existing.identity_path.clone());
        let default_base_path = input
            .default_base_path
            .or_else(|| existing.default_base_path.clone());
        let default_scratch_directory = existing.default_scratch_directory.clone();
        let port = input.port.unwrap_or(existing.port);
        let is_default = input.is_default.unwrap_or(existing.is_default);

        let cluster_name = input.name.clone();
        let result = self
            .cluster_upsert(
                ClusterUpsertInput {
                    name: input.name,
                    username,
                    address,
                    port,
                    identity_path,
                    default_base_path,
                    default_scratch_directory,
                    is_default,
                    emit_progress: false,
                },
                stream,
                mfa,
                None,
            )
            .await;
        if result.is_ok() {
            self.telemetry.event(
                "cluster.updated",
                TelemetryEvent {
                    cluster: Some(cluster_name),
                    user: event_user,
                    ..TelemetryEvent::default()
                },
            );
        }
        result
    }

    pub async fn check_running_jobs(&self, min_age: StdDuration) -> AppResult<()> {
        let jobs = self.jobs.list_running_jobs().await?;
        if jobs.is_empty() {
            return Ok(());
        }
        let hosts = self.clusters.list_hosts(None).await?;
        let min_age = TimeDuration::try_from(min_age).unwrap_or(TimeDuration::ZERO);
        let now = self.clock.now_utc();

        let mut host_map = HashMap::new();
        for host in hosts {
            host_map.insert(host.name.clone(), host);
        }

        let mut jobs_by_host: HashMap<String, Vec<JobRecord>> = HashMap::new();
        for job in jobs {
            jobs_by_host.entry(job.name.clone()).or_default().push(job);
        }

        let mut completed_ids: Vec<(i64, Option<String>)> = Vec::new();
        for (name, host_jobs) in jobs_by_host {
            let Some(host) = host_map.get(&name) else {
                tracing::warn!("host record missing for running job on '{name}'");
                continue;
            };
            let config = match self.config_for_host(host).await {
                Ok(cfg) => cfg,
                Err(err) => {
                    tracing::warn!("failed to resolve session for {name}: {err}");
                    continue;
                }
            };

            if self
                .remote_exec
                .needs_connect(&config)
                .await
                .unwrap_or(true)
            {
                let stream = NoopStreamOutput;
                let (mfa_tx, mfa_rx) = mpsc::channel::<proto::MfaAnswer>(1);
                drop(mfa_tx);
                let mut mfa_port = GrpcMfaPort { receiver: mfa_rx };
                if let Err(err) = self
                    .remote_exec
                    .ensure_connected(&config, &stream, &mut mfa_port)
                    .await
                {
                    tracing::warn!("failed to connect to {name} for job checks: {err}");
                    continue;
                }
            }

            for job in host_jobs {
                if min_age > TimeDuration::ZERO {
                    match OffsetDateTime::parse(&job.created_at, &Rfc3339) {
                        Ok(created_at) => {
                            if now - created_at < min_age {
                                continue;
                            }
                        }
                        Err(e) => {
                            tracing::debug!("failed to parse created_at for job {}: {}", job.id, e);
                        }
                    }
                }
                let Some(job_id) = job.scheduler_id else {
                    tracing::warn!("job {} has no scheduler id; skipping", job.id);
                    continue;
                };

                if host.accounting_available {
                    let command = format!("sacct -j {job_id} -n -P -o State");
                    let capture = match self.remote_exec.exec_capture(&config, &command).await {
                        Ok(v) => v,
                        Err(e) => {
                            tracing::warn!("sacct check failed on {name} for {job_id}: {e}");
                            continue;
                        }
                    };
                    if capture.exit_code != 0 {
                        let err_text = String::from_utf8_lossy(&capture.stderr);
                        tracing::warn!(
                            "sacct returned {} on {name} for {job_id}: {}",
                            capture.exit_code,
                            err_text
                        );
                        continue;
                    }
                    let output = String::from_utf8_lossy(&capture.stdout);
                    match slurm::sacct_terminal_state(&output) {
                        Some(v) => {
                            completed_ids.push((job.id, Some(v)));
                            continue;
                        }
                        None => {
                            tracing::debug!(
                                "sacct returned no terminal state for {name} job {job_id}"
                            );
                        }
                    };

                    let command = format!("squeue -j {job_id} -h -o %T");
                    let capture = match self.remote_exec.exec_capture(&config, &command).await {
                        Ok(v) => v,
                        Err(e) => {
                            tracing::warn!("squeue check failed on {name} for {job_id}: {e}");
                            continue;
                        }
                    };
                    if capture.exit_code != 0 {
                        tracing::warn!(
                            "squeue returned {} on {name} for {job_id}: {}",
                            capture.exit_code,
                            String::from_utf8_lossy(&capture.stderr)
                        );
                        continue;
                    }
                    let output = String::from_utf8_lossy(&capture.stdout);
                    if let Some(state) = parse_squeue_state(&output) {
                        if let Err(e) = self
                            .jobs
                            .update_job_scheduler_state(job.id, Some(&state))
                            .await
                        {
                            tracing::warn!(
                                "failed to update scheduler state for {name} job {job_id}: {e}"
                            );
                        }
                    }
                } else {
                    let command = format!("scontrol show job {job_id} -o");
                    match self.remote_exec.exec_capture(&config, &command).await {
                        Ok(capture) => {
                            if capture.exit_code == 0 {
                                let output = String::from_utf8_lossy(&capture.stdout);
                                if let Some(state) = slurm::scontrol_job_state(&output) {
                                    if slurm::slurm_state_is_active(&state) {
                                        if let Err(e) = self
                                            .jobs
                                            .update_job_scheduler_state(job.id, Some(&state))
                                            .await
                                        {
                                            tracing::warn!(
                                                "failed to update scheduler state for {name} job {job_id}: {e}"
                                            );
                                        }
                                        continue;
                                    }
                                    if slurm::slurm_state_is_terminal(&state) {
                                        completed_ids.push((job.id, Some(state)));
                                        continue;
                                    }
                                    tracing::debug!(
                                        "scontrol returned non-terminal state for {name} job {job_id}: {state}"
                                    );
                                    continue;
                                }
                                tracing::debug!(
                                    "scontrol returned no job state for {name} job {job_id}"
                                );
                            } else {
                                let err_text = String::from_utf8_lossy(&capture.stderr);
                                tracing::warn!(
                                    "scontrol returned {} on {name} for {job_id}: {}",
                                    capture.exit_code,
                                    err_text
                                );
                                if is_invalid_job_id(&err_text) {
                                    completed_ids.push((job.id, None));
                                    continue;
                                }
                            }
                        }
                        Err(e) => {
                            tracing::warn!("scontrol check failed on {name} for {job_id}: {e}");
                        }
                    }

                    let command = format!("squeue -j {job_id} -h -o %T");
                    let capture = match self.remote_exec.exec_capture(&config, &command).await {
                        Ok(v) => v,
                        Err(e) => {
                            tracing::warn!("squeue check failed on {name} for {job_id}: {e}");
                            continue;
                        }
                    };
                    if capture.exit_code != 0 {
                        let err_text = String::from_utf8_lossy(&capture.stderr);
                        tracing::warn!(
                            "squeue returned {} on {name} for {job_id}: {}",
                            capture.exit_code,
                            err_text
                        );
                        if is_invalid_job_id(&err_text) {
                            completed_ids.push((job.id, None));
                        }
                        continue;
                    }
                    let output = String::from_utf8_lossy(&capture.stdout);
                    if let Some(state) = parse_squeue_state(&output) {
                        if let Err(e) = self
                            .jobs
                            .update_job_scheduler_state(job.id, Some(&state))
                            .await
                        {
                            tracing::warn!(
                                "failed to update scheduler state for {name} job {job_id}: {e}"
                            );
                        }
                    } else {
                        completed_ids.push((job.id, None));
                    }
                }
            }
        }

        for (id, terminal_state) in completed_ids {
            if let Err(e) = self
                .jobs
                .mark_job_completed(id, terminal_state.as_deref())
                .await
            {
                tracing::warn!("failed to mark job {id} completed: {e}");
            }
        }
        Ok(())
    }

    async fn run_command(
        &self,
        name: &str,
        command: String,
        stream: &dyn StreamOutputPort,
        mfa: &mut dyn MfaPort,
    ) -> AppResult<()> {
        let config = self.config_for_host_name(name).await?;
        if let Err(_) = self
            .remote_exec
            .exec_stream(&config, &command, stream, mfa)
            .await
        {
            let _ = stream
                .send(StreamEvent {
                    event: Some(stream_event::Event::Error(codes::REMOTE_ERROR.to_string())),
                })
                .await;
        }
        Ok(())
    }

    async fn get_default_base_path(&self, name: &str) -> AppResult<String> {
        let host = match self.clusters.get_by_name(name).await? {
            Some(v) => v,
            None => {
                return Err(AppError::new(
                    AppErrorKind::InvalidArgument,
                    codes::NOT_FOUND,
                ));
            }
        };
        match host.default_base_path {
            Some(v) => Ok(v),
            None => Err(AppError::new(
                AppErrorKind::InvalidArgument,
                codes::INVALID_ARGUMENT,
            )),
        }
    }

    async fn config_for_host_name(&self, name: &str) -> AppResult<SshConfig> {
        let host = match self.clusters.get_by_name(name).await? {
            Some(v) => v,
            None => {
                return Err(AppError::new(
                    AppErrorKind::InvalidArgument,
                    codes::NOT_FOUND,
                ));
            }
        };
        self.config_for_host(&host).await
    }

    async fn config_for_host(&self, host: &HostRecord) -> AppResult<SshConfig> {
        let addr = self
            .network
            .resolve_host_addr(&host.address, host.port)
            .await
            .map_err(|err| {
                AppError::with_message(AppErrorKind::Internal, err.code(), err.message())
            })?;
        Ok(build_ssh_config(
            Some(host.name.clone()),
            host.username.clone(),
            &host.address,
            addr,
            host.identity_path.clone(),
        ))
    }

    async fn cluster_upsert(
        &self,
        input: ClusterUpsertInput,
        stream: &dyn StreamOutputPort,
        mfa: &mut dyn MfaPort,
        mut default_selection: Option<&mut mpsc::Receiver<proto::AddClusterDefaultSelection>>,
    ) -> AppResult<()> {
        let reachable = self
            .network
            .check_host_reachable(&input.address, input.port)
            .await?;
        if !reachable {
            return Err(AppError::new(AppErrorKind::Aborted, codes::NETWORK_ERROR));
        }
        let addr = self
            .network
            .resolve_host_addr(&input.address, input.port)
            .await?;
        let config = build_ssh_config(
            Some(input.name.clone()),
            input.username.clone(),
            &input.address,
            addr,
            input.identity_path.clone(),
        );

        self.remote_exec
            .ensure_connected(&config, stream, mfa)
            .await?;

        let home_dir = fetch_remote_home_dir(self.remote_exec.as_ref(), &config).await?;

        let capture = self
            .remote_exec
            .exec_capture(&config, managers::DETERMINE_HPC_WORKLOAD_MANAGERS_CMD)
            .await
            .map_err(|_| AppError::new(AppErrorKind::Aborted, codes::REMOTE_ERROR))?;
        if capture.exit_code != 0 {
            return Err(AppError::new(AppErrorKind::Aborted, codes::REMOTE_ERROR));
        }
        let out = String::from_utf8(capture.stdout)
            .map_err(|_| AppError::new(AppErrorKind::Aborted, codes::REMOTE_ERROR))?;
        let wlms = managers::parse_wlms(&out);
        if !wlms.contains(&managers::WorkloadManager::Slurm) {
            return Err(AppError::new(AppErrorKind::Aborted, codes::REMOTE_ERROR));
        }
        if input.emit_progress {
            send_add_cluster_progress(stream, "Scheduler: Slurm").await;
        }

        let capture = self
            .remote_exec
            .exec_capture(&config, os::GATHER_OS_INFO_CMD)
            .await
            .map_err(|_| AppError::new(AppErrorKind::Aborted, codes::REMOTE_ERROR))?;
        if capture.exit_code != 0 {
            return Err(AppError::new(AppErrorKind::Aborted, codes::REMOTE_ERROR));
        }
        let out = String::from_utf8(capture.stdout)
            .map_err(|_| AppError::new(AppErrorKind::Aborted, codes::REMOTE_ERROR))?;
        let os_info = os::parse_distro_info(&out)
            .map_err(|_| AppError::new(AppErrorKind::Aborted, codes::REMOTE_ERROR))?;
        if input.emit_progress {
            let os_label = format!("OS: {} {}", os_info.id.as_str(), os_info.version.as_str());
            send_add_cluster_progress(stream, &os_label).await;
            let kernel_label = format!("Kernel: {}", os_info.kernel.as_str());
            send_add_cluster_progress(stream, &kernel_label).await;
        }
        let distro_info = crate::app::types::Distro {
            name: os_info.id,
            version: os_info.version,
        };

        let capture = self
            .remote_exec
            .exec_capture(&config, slurm::DETERMINE_SLURM_VERSION_CMD)
            .await
            .map_err(|_| AppError::new(AppErrorKind::Aborted, codes::REMOTE_ERROR))?;
        if capture.exit_code != 0 {
            return Err(AppError::new(AppErrorKind::Aborted, codes::REMOTE_ERROR));
        }
        let out = String::from_utf8(capture.stdout)
            .map_err(|_| AppError::new(AppErrorKind::Aborted, codes::REMOTE_ERROR))?;
        let mut parts = out.split_whitespace();
        let _ = parts.next();
        let Some(version) = parts.next() else {
            return Err(AppError::new(AppErrorKind::Aborted, codes::REMOTE_ERROR));
        };
        let slurm_version = version
            .parse::<crate::app::types::SlurmVersion>()
            .map_err(|_| AppError::new(AppErrorKind::Aborted, codes::REMOTE_ERROR))?;
        if input.emit_progress {
            send_add_cluster_progress(stream, &format!("Slurm: {slurm_version}")).await;
        }

        let capture = self
            .remote_exec
            .exec_capture(&config, "scontrol show config")
            .await
            .map_err(|_| AppError::new(AppErrorKind::Aborted, codes::REMOTE_ERROR))?;
        if capture.exit_code != 0 {
            return Err(AppError::new(AppErrorKind::Aborted, codes::REMOTE_ERROR));
        }
        let config_text = String::from_utf8_lossy(&capture.stdout);
        let accounting_enabled =
            slurm::parse_accounting_enabled_from_scontrol(&config_text).unwrap_or(false);
        if input.emit_progress {
            let accounting_state = if accounting_enabled {
                "enabled"
            } else {
                "disabled"
            };
            send_add_cluster_progress(stream, &format!("Accounting: {accounting_state}")).await;
        }

        let resolved_default_base_path =
            resolve_default_base_path(input.default_base_path, &home_dir)?;
        let normalized_default_base_path = normalize_default_base_path(resolved_default_base_path)?;
        if let Some(ref dbp) = normalized_default_base_path {
            validate_and_prepare_default_base_path(self.remote_exec.as_ref(), &config, dbp).await?;
        }
        let resolved_default_scratch_directory =
            resolve_default_scratch_directory(input.default_scratch_directory, &home_dir)?;
        let normalized_default_scratch_directory =
            normalize_default_scratch_directory(resolved_default_scratch_directory)?;
        if let Some(ref path) = normalized_default_scratch_directory {
            let _ =
                validate_scratch_directory_access(self.remote_exec.as_ref(), &config, path).await?;
        }
        let is_default = match default_selection.as_deref_mut() {
            Some(selection) => Self::recv_cluster_default_selection(stream, selection).await?,
            None => input.is_default,
        };

        let new_host = NewHost {
            username: input.username,
            name: input.name.clone(),
            address: input.address,
            distro: distro_info,
            kernel_version: os_info.kernel,
            slurm: slurm_version,
            port: input.port,
            identity_path: input.identity_path,
            accounting_available: accounting_enabled,
            default_base_path: normalized_default_base_path,
            default_scratch_directory: normalized_default_scratch_directory,
            is_default,
        };

        if self.clusters.upsert_host(&new_host).await.is_err() {
            let _ = stream
                .send(StreamEvent {
                    event: Some(stream_event::Event::Error(
                        codes::INTERNAL_ERROR.to_string(),
                    )),
                })
                .await;
        }

        Ok(())
    }

    async fn recv_cluster_default_selection(
        stream: &dyn StreamOutputPort,
        selection: &mut mpsc::Receiver<proto::AddClusterDefaultSelection>,
    ) -> AppResult<bool> {
        stream
            .send(StreamEvent {
                event: Some(stream_event::Event::AddClusterDefaultPrompt(
                    proto::AddClusterDefaultPrompt {
                        current_default_name: None,
                    },
                )),
            })
            .await?;
        let Some(selected) = selection.recv().await else {
            return Err(AppError::new(AppErrorKind::Cancelled, codes::CANCELED));
        };
        Ok(selected.is_default)
    }
}

#[derive(Clone, Debug)]
pub struct ResolveHomeDirInput {
    pub username: String,
    pub address: Address,
    pub port: u16,
    pub identity_path: Option<String>,
    pub session_name: Option<String>,
}

#[derive(Clone, Debug)]
pub struct ResolveScratchDirectoriesInput {
    pub username: String,
    pub address: Address,
    pub port: u16,
    pub identity_path: Option<String>,
    pub session_name: Option<String>,
}

#[derive(Clone, Debug)]
pub struct ValidateScratchDirectoryInput {
    pub username: String,
    pub address: Address,
    pub port: u16,
    pub identity_path: Option<String>,
    pub session_name: Option<String>,
    pub directory: String,
}

#[derive(Clone, Debug)]
pub struct ValidateDefaultBasePathInput {
    pub username: String,
    pub address: Address,
    pub port: u16,
    pub identity_path: Option<String>,
    pub session_name: Option<String>,
    pub base_path: String,
}

#[derive(Clone, Debug)]
pub struct LsInput {
    pub name: String,
    pub path: Option<String>,
    pub job_id: Option<i64>,
}

#[derive(Clone, Debug)]
pub struct RetrieveJobInput {
    pub job_id: i64,
    pub path: String,
    pub local_path: Option<String>,
    pub overwrite: bool,
    pub force: bool,
}

#[derive(Clone, Debug)]
pub struct JobLogsInput {
    pub job_id: i64,
    pub stderr: bool,
}

#[derive(Clone, Debug)]
pub struct CancelJobInput {
    pub job_id: i64,
}

#[derive(Clone, Debug)]
pub struct CleanupJobInput {
    pub job_id: i64,
    pub force: bool,
    pub full: bool,
}

#[derive(Clone, Debug)]
pub struct RunJobInput {
    pub local_path: String,
    pub remote_path: Option<String>,
    pub name: String,
    pub sbatchscript: String,
    pub filters: Vec<crate::app::types::SyncFilterRule>,
    pub new_directory: bool,
    pub blueprint_name: Option<String>,
    pub default_retrieve_path: Option<String>,
    pub template_values_json: Option<String>,
}

#[derive(Clone, Debug)]
pub struct RunBlueprintInput {
    pub blueprint_name: String,
    pub blueprint_tag: String,
    pub remote_path: Option<String>,
    pub name: String,
    pub sbatchscript: String,
    pub filters: Vec<crate::app::types::SyncFilterRule>,
    pub new_directory: bool,
    pub default_retrieve_path: Option<String>,
    pub template_values_json: Option<String>,
}

#[derive(Clone, Debug)]
struct RunInput {
    pub local_path: String,
    pub remote_path: Option<String>,
    pub name: String,
    pub sbatchscript: String,
    pub filters: Vec<crate::app::types::SyncFilterRule>,
    pub new_directory: bool,
    pub blueprint_name: Option<String>,
    pub blueprint_tag: Option<String>,
    pub default_retrieve_path: Option<String>,
    pub template_values_json: Option<String>,
}

#[derive(Clone, Debug)]
pub struct AddClusterInput {
    pub name: String,
    pub username: String,
    pub address: Address,
    pub port: u16,
    pub identity_path: Option<String>,
    pub default_base_path: Option<String>,
    pub default_scratch_directory: Option<String>,
}

#[derive(Clone, Debug)]
pub struct SetClusterInput {
    pub name: String,
    pub address: Option<Address>,
    pub username: Option<String>,
    pub port: Option<u16>,
    pub identity_path: Option<String>,
    pub default_base_path: Option<String>,
    pub is_default: Option<bool>,
}

#[derive(Clone, Debug)]
struct ClusterUpsertInput {
    name: String,
    username: String,
    address: Address,
    port: u16,
    identity_path: Option<String>,
    default_base_path: Option<String>,
    default_scratch_directory: Option<String>,
    is_default: bool,
    emit_progress: bool,
}

struct GrpcMfaPort {
    receiver: mpsc::Receiver<proto::MfaAnswer>,
}

impl MfaPort for GrpcMfaPort {
    fn receiver(&mut self) -> &mut mpsc::Receiver<proto::MfaAnswer> {
        &mut self.receiver
    }
}

struct NoopStreamOutput;

#[async_trait::async_trait]
impl StreamOutputPort for NoopStreamOutput {
    async fn send(&self, _event: StreamEvent) -> AppResult<()> {
        Ok(())
    }

    fn is_closed(&self) -> bool {
        false
    }
}

async fn send_add_cluster_progress(stream: &dyn StreamOutputPort, message: &str) {
    let line = format!("✓ {message}\n");
    let _ = stream
        .send(StreamEvent {
            event: Some(stream_event::Event::Stderr(line.into_bytes())),
        })
        .await;
}

fn build_ssh_config(
    session_name: Option<String>,
    username: String,
    address: &Address,
    addr: SocketAddr,
    identity_path: Option<String>,
) -> SshConfig {
    let host = format_address(address);
    SshConfig {
        session_name,
        host,
        addr,
        username,
        identity_path,
        ki_submethods: None,
        keepalive_secs: 60,
    }
}

fn host_record_to_new_host(host: &HostRecord, is_default: bool) -> NewHost {
    NewHost {
        name: host.name.clone(),
        username: host.username.clone(),
        address: host.address.clone(),
        port: host.port,
        identity_path: host.identity_path.clone(),
        slurm: host.slurm,
        distro: host.distro.clone(),
        kernel_version: host.kernel_version.clone(),
        accounting_available: host.accounting_available,
        default_base_path: host.default_base_path.clone(),
        default_scratch_directory: host.default_scratch_directory.clone(),
        is_default,
    }
}

fn format_address(address: &Address) -> String {
    match address {
        Address::Hostname(host) => host.clone(),
        Address::Ip(ip) => ip.to_string(),
    }
}

async fn fetch_remote_home_dir(
    remote_exec: &dyn RemoteExecPort,
    config: &SshConfig,
) -> AppResult<String> {
    let command = "printf '%s' \"$HOME\"";
    let capture = remote_exec
        .exec_capture(config, command)
        .await
        .map_err(|_| AppError::new(AppErrorKind::Aborted, codes::REMOTE_ERROR))?;
    if capture.exit_code != 0 {
        return Err(AppError::new(AppErrorKind::Aborted, codes::REMOTE_ERROR));
    }
    let home_raw = String::from_utf8(capture.stdout)
        .map_err(|_| AppError::new(AppErrorKind::Aborted, codes::REMOTE_ERROR))?;
    let home = home_raw.trim();
    if home.is_empty() {
        return Err(AppError::new(AppErrorKind::Aborted, codes::REMOTE_ERROR));
    }
    if !Path::new(home).is_absolute() {
        return Err(AppError::new(AppErrorKind::Aborted, codes::REMOTE_ERROR));
    }
    Ok(home.to_string())
}

async fn fetch_remote_username(
    remote_exec: &dyn RemoteExecPort,
    config: &SshConfig,
) -> AppResult<String> {
    let command = "printf '%s' \"$USER\"";
    let capture = remote_exec
        .exec_capture(config, command)
        .await
        .map_err(|_| AppError::new(AppErrorKind::Aborted, codes::REMOTE_ERROR))?;
    if capture.exit_code != 0 {
        return Err(AppError::new(AppErrorKind::Aborted, codes::REMOTE_ERROR));
    }
    let user_raw = String::from_utf8(capture.stdout)
        .map_err(|_| AppError::new(AppErrorKind::Aborted, codes::REMOTE_ERROR))?;
    let user = user_raw.trim();
    if user.is_empty() {
        return Err(AppError::new(AppErrorKind::Aborted, codes::REMOTE_ERROR));
    }
    Ok(user.to_string())
}

fn resolve_default_base_path(
    default_base_path: Option<String>,
    home_dir: &str,
) -> AppResult<Option<String>> {
    let default_base_path = default_base_path.and_then(|value| {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    });

    let Some(raw) = default_base_path else {
        let expanded = PathBuf::from(home_dir).join("runs");
        return Ok(Some(expanded.to_string_lossy().into_owned()));
    };

    if raw == "~" {
        return Ok(Some(home_dir.to_string()));
    }

    if let Some(suffix) = raw.strip_prefix("~/") {
        if suffix.is_empty() {
            return Ok(Some(home_dir.to_string()));
        }
        let expanded = PathBuf::from(home_dir).join(suffix);
        return Ok(Some(expanded.to_string_lossy().into_owned()));
    }

    if raw.starts_with('~') {
        return Err(AppError::new(
            AppErrorKind::InvalidArgument,
            codes::INVALID_ARGUMENT,
        ));
    }

    Ok(Some(raw))
}

fn normalize_default_base_path(default_base_path: Option<String>) -> AppResult<Option<String>> {
    let Some(base) = default_base_path else {
        return Ok(None);
    };
    let normalized = remote_path::normalize_path(base);
    if normalized.is_absolute() {
        Ok(Some(normalized.to_string_lossy().into_owned()))
    } else {
        Err(AppError::new(
            AppErrorKind::InvalidArgument,
            codes::INVALID_ARGUMENT,
        ))
    }
}

fn resolve_default_scratch_directory(
    default_scratch_directory: Option<String>,
    home_dir: &str,
) -> AppResult<Option<String>> {
    let default_scratch_directory = default_scratch_directory.and_then(|value| {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    });

    let Some(raw) = default_scratch_directory else {
        return Ok(None);
    };

    if raw == "~" {
        return Ok(Some(home_dir.to_string()));
    }

    if let Some(suffix) = raw.strip_prefix("~/") {
        if suffix.is_empty() {
            return Ok(Some(home_dir.to_string()));
        }
        let expanded = PathBuf::from(home_dir).join(suffix);
        return Ok(Some(expanded.to_string_lossy().into_owned()));
    }

    if raw.starts_with('~') {
        return Err(invalid_argument(
            "scratch directory must be absolute or start with '~/'",
        ));
    }

    Ok(Some(raw))
}

fn normalize_default_scratch_directory(
    default_scratch_directory: Option<String>,
) -> AppResult<Option<String>> {
    let Some(path) = default_scratch_directory else {
        return Ok(None);
    };
    let normalized = remote_path::normalize_path(path);
    if normalized.is_absolute() {
        Ok(Some(normalized.to_string_lossy().into_owned()))
    } else {
        Err(invalid_argument(
            "scratch directory must be absolute or start with '~/'",
        ))
    }
}

async fn validate_scratch_directory_access(
    remote_exec: &dyn RemoteExecPort,
    config: &SshConfig,
    directory: &str,
) -> AppResult<String> {
    let directory_escaped = shell::sh_escape(directory);

    let exists_command = format!("test -d {directory_escaped}");
    let capture = remote_exec
        .exec_capture(config, &exists_command)
        .await
        .map_err(|_| AppError::new(AppErrorKind::Aborted, codes::REMOTE_ERROR))?;
    if capture.exit_code != 0 {
        return Err(invalid_argument(format!(
            "directory {directory} doesn't exist"
        )));
    }

    let resolve_command = format!("cd -- {directory_escaped} && pwd -P");
    let capture = remote_exec
        .exec_capture(config, &resolve_command)
        .await
        .map_err(|_| AppError::new(AppErrorKind::Aborted, codes::REMOTE_ERROR))?;
    if capture.exit_code != 0 {
        return Err(invalid_argument(format!(
            "directory {directory} can't be accessed"
        )));
    }
    let resolved = String::from_utf8(capture.stdout)
        .map_err(|_| AppError::new(AppErrorKind::Aborted, codes::REMOTE_ERROR))?;
    let resolved = resolved.trim().to_string();
    if resolved.is_empty() || !resolved.starts_with('/') {
        return Err(invalid_argument(format!(
            "directory {directory} can't be accessed"
        )));
    }
    tracing::debug!(
        "scratch directory resolved input={} resolved={}",
        directory,
        resolved
    );
    let resolved_escaped = shell::sh_escape(&resolved);

    let list_command = format!("ls -A -- {resolved_escaped} >/dev/null");
    let capture = remote_exec
        .exec_capture(config, &list_command)
        .await
        .map_err(|_| AppError::new(AppErrorKind::Aborted, codes::REMOTE_ERROR))?;
    if capture.exit_code != 0 {
        return Err(invalid_argument(format!(
            "directory {directory} can't be listed"
        )));
    }

    let probe_file = format!(
        "{}/.orbit-scratch-probe-{}",
        resolved.trim_end_matches('/'),
        random::generate_run_directory_name()
    );
    let probe_file_escaped = shell::sh_escape(&probe_file);

    let create_command = format!("touch -- {probe_file_escaped}");
    let capture = remote_exec
        .exec_capture(config, &create_command)
        .await
        .map_err(|_| AppError::new(AppErrorKind::Aborted, codes::REMOTE_ERROR))?;
    if capture.exit_code != 0 {
        return Err(invalid_argument(format!(
            "directory {directory} can't create files"
        )));
    }

    let write_command = format!("printf 'orbit' > {probe_file_escaped}");
    let capture = remote_exec
        .exec_capture(config, &write_command)
        .await
        .map_err(|_| AppError::new(AppErrorKind::Aborted, codes::REMOTE_ERROR))?;
    if capture.exit_code != 0 {
        let _ = remote_exec
            .exec_capture(config, &format!("rm -f -- {probe_file_escaped}"))
            .await;
        return Err(invalid_argument(format!(
            "directory {directory} can't create files"
        )));
    }

    let read_command = format!("cat -- {probe_file_escaped} >/dev/null");
    let capture = remote_exec
        .exec_capture(config, &read_command)
        .await
        .map_err(|_| AppError::new(AppErrorKind::Aborted, codes::REMOTE_ERROR))?;
    if capture.exit_code != 0 {
        let _ = remote_exec
            .exec_capture(config, &format!("rm -f -- {probe_file_escaped}"))
            .await;
        return Err(invalid_argument(format!(
            "directory {directory} can't read files"
        )));
    }

    let _ = remote_exec
        .exec_capture(config, &format!("rm -f -- {probe_file_escaped}"))
        .await;
    Ok(resolved)
}

async fn validate_and_prepare_default_base_path(
    remote_exec: &dyn RemoteExecPort,
    config: &SshConfig,
    base_path: &str,
) -> AppResult<()> {
    let base_path_escaped = shell::sh_escape(base_path);

    let exists_dir_command = format!("test -d {base_path_escaped}");
    let capture = remote_exec
        .exec_capture(config, &exists_dir_command)
        .await
        .map_err(|_| AppError::new(AppErrorKind::Aborted, codes::REMOTE_ERROR))?;

    if capture.exit_code != 0 {
        let exists_entry_command = format!("test -e {base_path_escaped}");
        let capture = remote_exec
            .exec_capture(config, &exists_entry_command)
            .await
            .map_err(|_| AppError::new(AppErrorKind::Aborted, codes::REMOTE_ERROR))?;
        if capture.exit_code == 0 {
            return Err(invalid_argument(format!(
                "default base path {base_path} exists but is not a directory"
            )));
        }

        let parent = Path::new(base_path)
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .map(|path| path.to_string_lossy().into_owned())
            .unwrap_or_else(|| "/".to_string());
        let parent_escaped = shell::sh_escape(&parent);

        let parent_exists_command = format!("test -d {parent_escaped}");
        let capture = remote_exec
            .exec_capture(config, &parent_exists_command)
            .await
            .map_err(|_| AppError::new(AppErrorKind::Aborted, codes::REMOTE_ERROR))?;
        if capture.exit_code != 0 {
            return Err(invalid_argument(format!(
                "parent directory {parent} doesn't exist; only one level can be created"
            )));
        }

        let parent_permissions_command =
            format!("test -w {parent_escaped} && test -x {parent_escaped}");
        let capture = remote_exec
            .exec_capture(config, &parent_permissions_command)
            .await
            .map_err(|_| AppError::new(AppErrorKind::Aborted, codes::REMOTE_ERROR))?;
        if capture.exit_code != 0 {
            return Err(invalid_argument(format!(
                "insufficient permissions to create {base_path} under {parent}"
            )));
        }

        // Create only the specified directory. Parent directories must already exist.
        let create_command = format!("mkdir -- {base_path_escaped}");
        let capture = remote_exec
            .exec_capture(config, &create_command)
            .await
            .map_err(|_| AppError::new(AppErrorKind::Aborted, codes::REMOTE_ERROR))?;
        if capture.exit_code != 0 {
            return Err(invalid_argument(format!(
                "default base path {base_path} can't be created"
            )));
        }
    }

    let resolve_command = format!("cd -- {base_path_escaped} && pwd -P");
    let capture = remote_exec
        .exec_capture(config, &resolve_command)
        .await
        .map_err(|_| AppError::new(AppErrorKind::Aborted, codes::REMOTE_ERROR))?;
    if capture.exit_code != 0 {
        return Err(invalid_argument(format!(
            "default base path {base_path} can't be accessed"
        )));
    }

    let resolved = String::from_utf8(capture.stdout)
        .map_err(|_| AppError::new(AppErrorKind::Aborted, codes::REMOTE_ERROR))?;
    let resolved = resolved.trim();
    if resolved.is_empty() || !resolved.starts_with('/') {
        return Err(invalid_argument(format!(
            "default base path {base_path} can't be accessed"
        )));
    }

    Ok(())
}

fn resolve_retrieve_local_target(
    path: &str,
    remote_path: &str,
    local_base: &Path,
    path_is_absolute: bool,
) -> Option<PathBuf> {
    if path_is_absolute {
        Path::new(remote_path)
            .file_name()
            .map(|name| local_base.join(name))
    } else {
        match Path::new(path).file_name() {
            Some(name) => Some(local_base.join(name)),
            None => Some(local_base.to_path_buf()),
        }
    }
}

fn parse_squeue_state(output: &str) -> Option<String> {
    output
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .map(|state| state.to_ascii_uppercase())
}

fn is_invalid_job_id(text: &str) -> bool {
    text.to_ascii_lowercase().contains("invalid job id")
}

fn invalid_argument(message: impl Into<String>) -> AppError {
    AppError::with_message(
        AppErrorKind::InvalidArgument,
        codes::INVALID_ARGUMENT,
        message,
    )
}

fn local_error(message: impl Into<String>) -> AppError {
    AppError::with_message(AppErrorKind::Internal, codes::LOCAL_ERROR, message)
}

async fn resolve_blueprint_path(fs: &dyn LocalFilesystemPort, raw: &str) -> AppResult<PathBuf> {
    let input = PathBuf::from(raw);
    let path = if input.is_absolute() {
        input
    } else {
        fs.current_dir().await?.join(input)
    };
    std::fs::canonicalize(&path)
        .map_err(|err| local_error(format!("failed to resolve blueprint path: {err}")))
}

fn discover_orbitfile_root(start: &Path) -> AppResult<Option<PathBuf>> {
    let mut cursor = if start.is_dir() {
        start.to_path_buf()
    } else {
        start
            .parent()
            .map(Path::to_path_buf)
            .ok_or_else(|| invalid_argument("run path has no parent directory"))?
    };

    loop {
        let orbitfile = cursor.join(ORBITFILE_NAME);
        if orbitfile.is_file() {
            return Ok(Some(cursor));
        }
        if !cursor.pop() {
            break;
        }
    }
    Ok(None)
}

#[derive(Deserialize)]
struct RawOrbitfile {
    #[serde(default)]
    blueprint: Option<RawBlueprint>,
    #[serde(default)]
    retrieve: Option<RawRetrieve>,
    #[serde(default)]
    submit: Option<RawSubmit>,
    #[serde(default)]
    sync: Option<RawSync>,
}

#[derive(Deserialize)]
struct RawBlueprint {
    name: String,
}

#[derive(Deserialize)]
struct RawRetrieve {
    #[serde(default)]
    default_path: Option<String>,
}

#[derive(Deserialize)]
struct RawSubmit {
    #[serde(default)]
    sbatch_script: Option<String>,
}

#[derive(Deserialize)]
struct RawSync {
    #[serde(default)]
    include: Vec<String>,
    #[serde(default)]
    exclude: Vec<String>,
}

struct OrbitfileMetadata {
    name: String,
    default_retrieve_path: Option<String>,
    submit_sbatch_script: Option<String>,
    sync_include: Vec<String>,
    sync_exclude: Vec<String>,
}

fn load_orbitfile_metadata(orbitfile_path: &Path) -> AppResult<OrbitfileMetadata> {
    let content = std::fs::read_to_string(orbitfile_path).map_err(|err| {
        local_error(format!(
            "failed to read Orbitfile {}: {err}",
            orbitfile_path.display()
        ))
    })?;
    let raw = toml::from_str::<RawOrbitfile>(&content).map_err(|err| {
        invalid_argument(format!(
            "invalid Orbitfile {}: {err}",
            orbitfile_path.display()
        ))
    })?;
    let blueprint = raw
        .blueprint
        .ok_or_else(|| invalid_argument("Orbitfile is missing [blueprint]"))?;
    let name = blueprint.name.trim().to_string();
    if name.is_empty() {
        return Err(invalid_argument("Orbitfile is missing [blueprint].name"));
    }

    let default_retrieve_path = trim_optional(raw.retrieve.and_then(|v| v.default_path));
    let submit_sbatch_script = trim_optional(raw.submit.and_then(|v| v.sbatch_script));

    let mut sync_include = Vec::new();
    let mut sync_exclude = Vec::new();
    if let Some(sync) = raw.sync {
        sync_include = normalize_patterns(sync.include, "sync.include")?;
        sync_exclude = normalize_patterns(sync.exclude, "sync.exclude")?;
    }

    Ok(OrbitfileMetadata {
        name,
        default_retrieve_path,
        submit_sbatch_script,
        sync_include,
        sync_exclude,
    })
}

fn validate_blueprint_name(name: &str) -> AppResult<()> {
    let trimmed = name.trim();
    if trimmed.is_empty() {
        return Err(invalid_argument("blueprint name cannot be empty"));
    }
    let valid = trimmed
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '_');
    if !valid {
        return Err(invalid_argument(
            "blueprint name must match ^[A-Za-z0-9_-]+$",
        ));
    }
    Ok(())
}

fn resolve_orbitfile_sbatch_script(
    project_root: &Path,
    submit_root: &Path,
    sbatch_script: &str,
) -> AppResult<String> {
    let trimmed = sbatch_script.trim();
    if trimmed.is_empty() {
        return Err(invalid_argument("[submit].sbatch_script cannot be empty"));
    }
    let submit_root_canonical = std::fs::canonicalize(submit_root).map_err(|err| {
        local_error(format!(
            "failed to resolve submit root '{}': {err}",
            submit_root.display()
        ))
    })?;
    let candidate = PathBuf::from(trimmed);
    let candidate = if candidate.is_absolute() {
        candidate
    } else {
        project_root.join(candidate)
    };
    let script_canonical = std::fs::canonicalize(&candidate).map_err(|err| {
        invalid_argument(format!(
            "invalid [submit].sbatch_script '{}': {err}",
            trimmed
        ))
    })?;
    if !script_canonical.starts_with(&submit_root_canonical) {
        return Err(invalid_argument(format!(
            "invalid [submit].sbatch_script '{}': path must resolve inside submit root '{}'",
            trimmed,
            submit_root.display()
        )));
    }
    if !script_canonical.is_file() {
        return Err(invalid_argument(format!(
            "invalid [submit].sbatch_script '{}': target is not a file",
            trimmed
        )));
    }
    let rel = script_canonical
        .strip_prefix(&submit_root_canonical)
        .map_err(|_| invalid_argument("failed to relativize sbatch script path"))?;
    if rel.as_os_str().is_empty() {
        return Err(invalid_argument(
            "invalid [submit].sbatch_script: relative path is empty",
        ));
    }
    Ok(rel.to_string_lossy().replace('\\', "/"))
}

fn collect_sbatch_scripts(root: &Path) -> AppResult<Vec<String>> {
    let mut scripts = Vec::new();
    for entry in WalkDir::new(root).follow_links(false) {
        let entry =
            entry.map_err(|err| local_error(format!("failed to walk blueprint root: {err}")))?;
        if entry.file_type().is_symlink() || !entry.file_type().is_file() {
            continue;
        }
        let path = entry.path();
        if path.extension().map(|ext| ext == "sbatch").unwrap_or(false) {
            let rel = path.strip_prefix(root).map_err(|_| {
                local_error("failed to compute sbatch script relative path".to_string())
            })?;
            if !rel.as_os_str().is_empty() {
                scripts.push(rel.to_string_lossy().into_owned());
            }
        }
    }
    scripts.sort();
    Ok(scripts)
}

fn trim_optional(value: Option<String>) -> Option<String> {
    value.and_then(|item| {
        let trimmed = item.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
}

fn normalize_patterns(values: Vec<String>, field: &str) -> AppResult<Vec<String>> {
    let mut out = Vec::with_capacity(values.len());
    for value in values {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            return Err(invalid_argument(format!(
                "Orbitfile {field} contains an empty pattern"
            )));
        }
        out.push(trimmed.to_string());
    }
    Ok(out)
}

fn split_blueprint_ref(value: &str) -> Option<(&str, &str)> {
    let trimmed = value.trim();
    let (name, tag) = trimmed.split_once(':')?;
    let name = name.trim();
    let tag = tag.trim();
    if name.is_empty() || tag.is_empty() {
        None
    } else {
        Some((name, tag))
    }
}

fn tarball_file_hash_from_blueprint_ref(blueprint_ref: &str) -> String {
    // blueprint name:tag ref
    let mut hasher = Sha256::new();
    hasher.update(blueprint_ref);
    format!("{:x}", hasher.finalize())
}

fn tarball_path_for_blueprint_ref(tarballs_dir: &Path, blueprint_ref: &str) -> PathBuf {
    // tarball path from blueprint reference (hashes blueprint name:tag, joins tarballs dir to it)
    let file_hash = tarball_file_hash_from_blueprint_ref(blueprint_ref);
    tarballs_dir.join(format!("{file_hash}.tar.zst"))
}

fn tarball_path_for_blueprint_record(
    tarballs_dir: &Path,
    blueprint: &BlueprintRecord,
) -> AppResult<PathBuf> {
    let version_tag = blueprint
        .version_tag
        .as_deref()
        .ok_or_else(|| invalid_argument("blueprint build is missing version tag"))?;
    let blueprint_ref = format!("{}:{version_tag}", blueprint.name);
    Ok(tarball_path_for_blueprint_ref(tarballs_dir, &blueprint_ref))
}

impl UseCases {
    async fn get_latest_blueprint_build(&self, blueprint_name: &str) -> AppResult<BlueprintRecord> {
        match self
            .projects
            .get_latest_blueprint_build(blueprint_name)
            .await?
        {
            Some(blueprint) => Ok(blueprint),
            None => Err(AppError::new(
                AppErrorKind::InvalidArgument,
                codes::NOT_FOUND,
            )),
        }
    }

    async fn next_blueprint_version_tag(
        &self,
        blueprint_name: &str,
    ) -> AppResult<(String, String)> {
        let now = self.clock.now_utc();
        let date = format!(
            "{:04}{:02}{:02}",
            now.year(),
            u8::from(now.month()),
            now.day()
        );
        let max = self
            .projects
            .max_build_number_for_date(blueprint_name, &date)
            .await?;
        let next = match max {
            Some(value) => {
                if value >= 999 {
                    return Err(invalid_argument(format!(
                        "build number exhausted for {blueprint_name} on {date}"
                    )));
                }
                value + 1
            }
            None => 0,
        };
        let version_tag = format!("{date}.{next:03}");
        let blueprint_ref = format!("{blueprint_name}:{version_tag}");
        Ok((version_tag, blueprint_ref))
    }
}

fn is_unsafe_cleanup_path(remote_path: &str) -> bool {
    let trimmed = remote_path.trim();
    if trimmed.is_empty()
        || trimmed == "."
        || trimmed == "/"
        || trimmed == "~"
        || trimmed.starts_with("~/")
        || trimmed.starts_with('~')
    {
        return true;
    }
    let normalized = remote_path::normalize_path(trimmed);
    normalized.as_os_str().is_empty() || normalized == Path::new(std::path::MAIN_SEPARATOR_STR)
}

fn normalize_blueprint_filter(blueprint_filter: Option<&str>) -> AppResult<Option<String>> {
    match blueprint_filter {
        Some(value) => {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                return Err(invalid_argument("blueprint name filter cannot be empty"));
            }
            Ok(Some(trimmed.to_string()))
        }
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Mutex};
    use tokio::sync::{mpsc, watch};

    use crate::app::ports::ExecCapture;
    use crate::app::types::{Distro, NewBlueprintBuild, NewJob, SlurmVersion};

    #[derive(Default)]
    struct NoopRemoteExec;

    #[async_trait::async_trait]
    impl RemoteExecPort for NoopRemoteExec {
        async fn exec_capture(
            &self,
            _config: &SshConfig,
            _command: &str,
        ) -> AppResult<ExecCapture> {
            panic!("exec_capture should not be called in list_jobs tests");
        }

        async fn exec_stream(
            &self,
            _config: &SshConfig,
            _command: &str,
            _stream: &dyn StreamOutputPort,
            _mfa: &mut dyn MfaPort,
        ) -> AppResult<()> {
            panic!("exec_stream should not be called in list_jobs tests");
        }

        async fn ensure_connected(
            &self,
            _config: &SshConfig,
            _stream: &dyn StreamOutputPort,
            _mfa: &mut dyn MfaPort,
        ) -> AppResult<()> {
            panic!("ensure_connected should not be called in list_jobs tests");
        }

        async fn ensure_connected_submit(
            &self,
            _config: &SshConfig,
            _stream: &dyn RunStreamOutputPort,
            _mfa: &mut dyn MfaPort,
        ) -> AppResult<()> {
            panic!("ensure_connected_submit should not be called in list_jobs tests");
        }

        async fn needs_connect(&self, _config: &SshConfig) -> AppResult<bool> {
            panic!("needs_connect should not be called in list_jobs tests");
        }

        async fn directory_exists(
            &self,
            _config: &SshConfig,
            _remote_dir: &str,
        ) -> AppResult<bool> {
            panic!("directory_exists should not be called in list_jobs tests");
        }

        async fn send_keepalive(&self, _session_name: &str) -> AppResult<()> {
            panic!("send_keepalive should not be called in list_jobs tests");
        }

        async fn is_connected(&self, _session_name: &str) -> AppResult<bool> {
            panic!("is_connected should not be called in list_jobs tests");
        }

        async fn remove_session(&self, _session_name: &str) -> AppResult<bool> {
            panic!("remove_session should not be called in list_jobs tests");
        }
    }

    #[derive(Default)]
    struct DeleteClusterRemoteExec;

    #[async_trait::async_trait]
    impl RemoteExecPort for DeleteClusterRemoteExec {
        async fn exec_capture(
            &self,
            _config: &SshConfig,
            _command: &str,
        ) -> AppResult<ExecCapture> {
            panic!("exec_capture should not be called in delete_cluster tests");
        }

        async fn exec_stream(
            &self,
            _config: &SshConfig,
            _command: &str,
            _stream: &dyn StreamOutputPort,
            _mfa: &mut dyn MfaPort,
        ) -> AppResult<()> {
            panic!("exec_stream should not be called in delete_cluster tests");
        }

        async fn ensure_connected(
            &self,
            _config: &SshConfig,
            _stream: &dyn StreamOutputPort,
            _mfa: &mut dyn MfaPort,
        ) -> AppResult<()> {
            panic!("ensure_connected should not be called in delete_cluster tests");
        }

        async fn ensure_connected_submit(
            &self,
            _config: &SshConfig,
            _stream: &dyn RunStreamOutputPort,
            _mfa: &mut dyn MfaPort,
        ) -> AppResult<()> {
            panic!("ensure_connected_submit should not be called in delete_cluster tests");
        }

        async fn needs_connect(&self, _config: &SshConfig) -> AppResult<bool> {
            panic!("needs_connect should not be called in delete_cluster tests");
        }

        async fn directory_exists(
            &self,
            _config: &SshConfig,
            _remote_dir: &str,
        ) -> AppResult<bool> {
            panic!("directory_exists should not be called in delete_cluster tests");
        }

        async fn send_keepalive(&self, _session_name: &str) -> AppResult<()> {
            panic!("send_keepalive should not be called in delete_cluster tests");
        }

        async fn is_connected(&self, _session_name: &str) -> AppResult<bool> {
            panic!("is_connected should not be called in delete_cluster tests");
        }

        async fn remove_session(&self, _session_name: &str) -> AppResult<bool> {
            Ok(true)
        }
    }

    struct ScriptedRemoteExec {
        capture: Mutex<Option<AppResult<ExecCapture>>>,
        needs_connect: bool,
        ensure_connected_error: Option<AppError>,
        ensure_connected_calls: Mutex<usize>,
        keepalive_calls: Mutex<usize>,
    }

    impl ScriptedRemoteExec {
        fn new(
            capture: AppResult<ExecCapture>,
            needs_connect: bool,
            ensure_connected_error: Option<AppError>,
        ) -> Self {
            Self {
                capture: Mutex::new(Some(capture)),
                needs_connect,
                ensure_connected_error,
                ensure_connected_calls: Mutex::new(0),
                keepalive_calls: Mutex::new(0),
            }
        }

        fn ensure_connected_calls(&self) -> usize {
            *self
                .ensure_connected_calls
                .lock()
                .expect("ensure_connected_calls lock")
        }

        fn keepalive_calls(&self) -> usize {
            *self.keepalive_calls.lock().expect("keepalive_calls lock")
        }
    }

    #[async_trait::async_trait]
    impl RemoteExecPort for ScriptedRemoteExec {
        async fn exec_capture(
            &self,
            _config: &SshConfig,
            _command: &str,
        ) -> AppResult<ExecCapture> {
            self.capture
                .lock()
                .expect("capture lock")
                .take()
                .expect("capture result should be present")
        }

        async fn exec_stream(
            &self,
            _config: &SshConfig,
            _command: &str,
            _stream: &dyn StreamOutputPort,
            _mfa: &mut dyn MfaPort,
        ) -> AppResult<()> {
            panic!("exec_stream should not be called in list_partitions tests");
        }

        async fn ensure_connected(
            &self,
            _config: &SshConfig,
            _stream: &dyn StreamOutputPort,
            _mfa: &mut dyn MfaPort,
        ) -> AppResult<()> {
            *self
                .ensure_connected_calls
                .lock()
                .expect("ensure_connected_calls lock") += 1;
            if let Some(err) = self.ensure_connected_error.clone() {
                return Err(err);
            }
            Ok(())
        }

        async fn ensure_connected_submit(
            &self,
            _config: &SshConfig,
            _stream: &dyn RunStreamOutputPort,
            _mfa: &mut dyn MfaPort,
        ) -> AppResult<()> {
            panic!("ensure_connected_submit should not be called in list_partitions tests");
        }

        async fn needs_connect(&self, _config: &SshConfig) -> AppResult<bool> {
            Ok(self.needs_connect)
        }

        async fn directory_exists(
            &self,
            _config: &SshConfig,
            _remote_dir: &str,
        ) -> AppResult<bool> {
            panic!("directory_exists should not be called in list_partitions tests");
        }

        async fn send_keepalive(&self, _session_name: &str) -> AppResult<()> {
            *self.keepalive_calls.lock().expect("keepalive_calls lock") += 1;
            Ok(())
        }

        async fn is_connected(&self, _session_name: &str) -> AppResult<bool> {
            panic!("is_connected should not be called in list_partitions tests");
        }

        async fn remove_session(&self, _session_name: &str) -> AppResult<bool> {
            panic!("remove_session should not be called in list_partitions tests");
        }
    }

    struct SequencedRemoteExec {
        captures: Mutex<VecDeque<(String, AppResult<ExecCapture>)>>,
    }

    impl SequencedRemoteExec {
        fn new(captures: Vec<(String, AppResult<ExecCapture>)>) -> Self {
            Self {
                captures: Mutex::new(VecDeque::from(captures)),
            }
        }
    }

    #[async_trait::async_trait]
    impl RemoteExecPort for SequencedRemoteExec {
        async fn exec_capture(&self, _config: &SshConfig, command: &str) -> AppResult<ExecCapture> {
            let mut captures = self.captures.lock().expect("captures lock");
            let Some((expected, result)) = captures.pop_front() else {
                panic!("unexpected command: {command}");
            };
            assert_eq!(command, expected);
            result
        }

        async fn exec_stream(
            &self,
            _config: &SshConfig,
            _command: &str,
            _stream: &dyn StreamOutputPort,
            _mfa: &mut dyn MfaPort,
        ) -> AppResult<()> {
            panic!("exec_stream should not be called in base-path validation tests");
        }

        async fn ensure_connected(
            &self,
            _config: &SshConfig,
            _stream: &dyn StreamOutputPort,
            _mfa: &mut dyn MfaPort,
        ) -> AppResult<()> {
            panic!("ensure_connected should not be called in base-path validation tests");
        }

        async fn ensure_connected_submit(
            &self,
            _config: &SshConfig,
            _stream: &dyn RunStreamOutputPort,
            _mfa: &mut dyn MfaPort,
        ) -> AppResult<()> {
            panic!("ensure_connected_submit should not be called in base-path validation tests");
        }

        async fn needs_connect(&self, _config: &SshConfig) -> AppResult<bool> {
            panic!("needs_connect should not be called in base-path validation tests");
        }

        async fn directory_exists(
            &self,
            _config: &SshConfig,
            _remote_dir: &str,
        ) -> AppResult<bool> {
            panic!("directory_exists should not be called in base-path validation tests");
        }

        async fn send_keepalive(&self, _session_name: &str) -> AppResult<()> {
            panic!("send_keepalive should not be called in base-path validation tests");
        }

        async fn is_connected(&self, _session_name: &str) -> AppResult<bool> {
            panic!("is_connected should not be called in base-path validation tests");
        }

        async fn remove_session(&self, _session_name: &str) -> AppResult<bool> {
            panic!("remove_session should not be called in base-path validation tests");
        }
    }

    #[derive(Default)]
    struct NoopFileSync;

    #[async_trait::async_trait]
    impl FileSyncPort for NoopFileSync {
        async fn sync_dir(
            &self,
            _config: &SshConfig,
            _local_dir: &Path,
            _remote_dir: &str,
            _options: SyncOptions,
            _stream: &dyn RunStreamOutputPort,
            _mfa: &mut dyn MfaPort,
        ) -> AppResult<()> {
            panic!("sync_dir should not be called in list_jobs tests");
        }

        async fn retrieve_path(
            &self,
            _config: &SshConfig,
            _remote_path: &str,
            _local_path: &Path,
            _overwrite: bool,
        ) -> AppResult<()> {
            panic!("retrieve_path should not be called in list_jobs tests");
        }
    }

    struct NoopRunStreamOutput;

    #[async_trait::async_trait]
    impl RunStreamOutputPort for NoopRunStreamOutput {
        async fn send(&self, _event: RunStreamEvent) -> AppResult<()> {
            Ok(())
        }

        fn is_closed(&self) -> bool {
            false
        }
    }

    struct NoopMfa {
        receiver: mpsc::Receiver<proto::MfaAnswer>,
    }

    impl NoopMfa {
        fn new() -> Self {
            let (_tx, receiver) = mpsc::channel(1);
            Self { receiver }
        }
    }

    impl MfaPort for NoopMfa {
        fn receiver(&mut self) -> &mut mpsc::Receiver<proto::MfaAnswer> {
            &mut self.receiver
        }
    }

    fn sample_host(name: &str) -> NewHost {
        NewHost {
            name: name.to_string(),
            username: "alice".to_string(),
            address: Address::Hostname(format!("{name}.example.com")),
            port: 22,
            identity_path: None,
            slurm: SlurmVersion {
                major: 23,
                minor: 11,
                patch: 0,
            },
            distro: Distro {
                name: "ubuntu".to_string(),
                version: "22.04".to_string(),
            },
            kernel_version: "6.8.0".to_string(),
            accounting_available: true,
            default_base_path: Some("/home/alice/runs".to_string()),
            default_scratch_directory: Some("/scratch/alice".to_string()),
            is_default: false,
        }
    }

    fn sample_job(host_id: i64, local_path: &str, blueprint_name: Option<&str>) -> NewJob {
        NewJob {
            scheduler_id: None,
            host_id,
            local_path: Some(local_path.to_string()),
            remote_path: format!("/remote/{local_path}"),
            stdout_path: format!("/remote/{local_path}/slurm.out"),
            stderr_path: None,
            blueprint_name: blueprint_name.map(str::to_string),
            default_retrieve_path: None,
            template_values: None,
        }
    }

    fn sample_localhost_host(name: &str) -> NewHost {
        let mut host = sample_host(name);
        host.address = Address::Hostname("localhost".to_string());
        host
    }

    async fn build_usecases_with_remote_exec(remote_exec: Arc<dyn RemoteExecPort>) -> UseCases {
        let store = crate::adapters::db::HostStore::open_memory()
            .await
            .expect("in-memory host store");
        let db = Arc::new(crate::adapters::db::SqliteStoreAdapter::new(store));
        let clusters: Arc<dyn ClusterStorePort> = db.clone();
        let jobs: Arc<dyn JobStorePort> = db.clone();
        let projects: Arc<dyn BlueprintStorePort> = db;
        let file_sync: Arc<dyn FileSyncPort> = Arc::new(NoopFileSync);
        let local_fs: Arc<dyn LocalFilesystemPort> = Arc::new(crate::adapters::fs::LocalFilesystem);
        let network: Arc<dyn NetworkProbePort> = Arc::new(crate::adapters::network::NetworkAdapter);
        let clock: Arc<dyn ClockPort> = Arc::new(crate::adapters::time::SystemClock);
        let telemetry: Arc<dyn TelemetryPort> = Arc::new(crate::app::ports::NoopTelemetry);
        UseCases::new(
            clusters,
            jobs,
            projects,
            remote_exec,
            file_sync,
            local_fs,
            network,
            clock,
            telemetry,
            std::env::temp_dir(),
        )
    }

    async fn build_usecases_for_list_jobs_tests() -> UseCases {
        let remote_exec: Arc<dyn RemoteExecPort> = Arc::new(NoopRemoteExec);
        build_usecases_with_remote_exec(remote_exec).await
    }

    async fn seed_jobs_for_blueprint_filtering(usecases: &UseCases) {
        let host_a_id = usecases
            .clusters
            .upsert_host(&sample_host("cluster-a"))
            .await
            .expect("insert cluster-a");
        let host_b_id = usecases
            .clusters
            .upsert_host(&sample_host("cluster-b"))
            .await
            .expect("insert cluster-b");

        usecases
            .jobs
            .insert_job(&sample_job(host_a_id, "run-a-demo", Some("demo-project")))
            .await
            .expect("insert cluster-a demo job");
        usecases
            .jobs
            .insert_job(&sample_job(host_a_id, "run-a-other", Some("other-project")))
            .await
            .expect("insert cluster-a other job");
        usecases
            .jobs
            .insert_job(&sample_job(host_b_id, "run-b-demo", Some("demo-project")))
            .await
            .expect("insert cluster-b demo job");
        usecases
            .jobs
            .insert_job(&sample_job(host_b_id, "run-b-none", None))
            .await
            .expect("insert cluster-b no-project job");
    }

    #[tokio::test]
    async fn run_job_rejects_running_combination_without_directory_override() {
        let usecases = build_usecases_for_list_jobs_tests().await;
        let local_dir = tempfile::tempdir().expect("temp dir");
        let local_path = local_dir.path().to_string_lossy().into_owned();
        let host_id = usecases
            .clusters
            .upsert_host(&sample_localhost_host("cluster-a"))
            .await
            .expect("insert cluster-a");
        let running_job = NewJob {
            scheduler_id: Some(401),
            host_id,
            local_path: Some(local_path.clone()),
            remote_path: "/remote/project".to_string(),
            stdout_path: "/remote/project/slurm-401.out".to_string(),
            stderr_path: None,
            blueprint_name: None,
            default_retrieve_path: None,
            template_values: None,
        };
        let running_job_id = usecases
            .jobs
            .insert_job(&running_job)
            .await
            .expect("insert running job");

        let stream = NoopRunStreamOutput;
        let mut mfa = NoopMfa::new();
        let (_cancel_tx, cancel_rx) = watch::channel(false);
        let err = usecases
            .run_job(
                RunJobInput {
                    local_path: local_path.clone(),
                    remote_path: None,
                    name: "cluster-a".to_string(),
                    sbatchscript: "job.sbatch".to_string(),
                    filters: Vec::new(),
                    new_directory: false,
                    blueprint_name: None,
                    default_retrieve_path: None,
                    template_values_json: None,
                },
                &stream,
                &mut mfa,
                cancel_rx,
            )
            .await
            .expect_err("run must be blocked");

        assert_eq!(err.kind(), AppErrorKind::AlreadyExists);
        assert_eq!(err.code(), codes::CONFLICT);
        assert!(err.message().contains(&format!("job {running_job_id}")));
        assert!(err.message().contains("--new-directory"));
        assert!(err.message().contains("--remote-path"));
    }

    #[tokio::test]
    async fn run_blueprint_rejects_running_combination_without_directory_override() {
        let usecases = build_usecases_for_list_jobs_tests().await;
        let host_id = usecases
            .clusters
            .upsert_host(&sample_localhost_host("cluster-a"))
            .await
            .expect("insert cluster-a");

        let blueprint_name = "demo-combo";
        let blueprint_tag = "20260216.001";
        let blueprint_ref = format!("{blueprint_name}:{blueprint_tag}");

        let source_root_dir = tempfile::tempdir().expect("source root temp dir");
        let source_root = source_root_dir.path().join("demo-combo");
        std::fs::create_dir_all(&source_root).expect("create source root");
        std::fs::write(source_root.join("job.sbatch"), "#!/bin/bash\n")
            .expect("write sbatch script");

        let tarball_path = tarball_path_for_blueprint_ref(&std::env::temp_dir(), &blueprint_ref);
        crate::app::services::project_building::create_tarball(
            &source_root,
            "demo-combo",
            &tarball_path,
            false,
        )
        .expect("create tarball");

        usecases
            .projects
            .upsert_blueprint_build(&NewBlueprintBuild {
                name: blueprint_ref,
                tarball_hash: "test-hash".to_string(),
                tarball_hash_function: "blake3".to_string(),
                tool_version: "test".to_string(),
                template_config_json: None,
                submit_sbatch_script: Some("job.sbatch".to_string()),
                sbatch_scripts: vec!["job.sbatch".to_string()],
                default_retrieve_path: None,
                sync_include: Vec::new(),
                sync_exclude: Vec::new(),
            })
            .await
            .expect("insert blueprint build");

        let running_job = NewJob {
            scheduler_id: Some(402),
            host_id,
            local_path: None,
            remote_path: "/remote/blueprint".to_string(),
            stdout_path: "/remote/blueprint/slurm-402.out".to_string(),
            stderr_path: None,
            blueprint_name: Some(blueprint_name.to_string()),
            default_retrieve_path: None,
            template_values: None,
        };
        let running_job_id = usecases
            .jobs
            .insert_job(&running_job)
            .await
            .expect("insert running job");

        let stream = NoopRunStreamOutput;
        let mut mfa = NoopMfa::new();
        let (_cancel_tx, cancel_rx) = watch::channel(false);
        let err = usecases
            .run_blueprint(
                RunBlueprintInput {
                    blueprint_name: blueprint_name.to_string(),
                    blueprint_tag: blueprint_tag.to_string(),
                    remote_path: None,
                    name: "cluster-a".to_string(),
                    sbatchscript: "job.sbatch".to_string(),
                    filters: Vec::new(),
                    new_directory: false,
                    default_retrieve_path: None,
                    template_values_json: None,
                },
                &stream,
                &mut mfa,
                cancel_rx,
            )
            .await
            .expect_err("run must be blocked");

        assert_eq!(err.kind(), AppErrorKind::AlreadyExists);
        assert_eq!(err.code(), codes::CONFLICT);
        assert!(err.message().contains(&format!("job {running_job_id}")));
        assert!(err.message().contains(blueprint_name));
        assert!(err.message().contains("--new-directory"));
        assert!(err.message().contains("--remote-path"));
    }

    #[tokio::test]
    async fn list_jobs_filters_by_blueprint_without_cluster_filter() {
        let usecases = build_usecases_for_list_jobs_tests().await;
        seed_jobs_for_blueprint_filtering(&usecases).await;

        let jobs = usecases
            .list_jobs(None, Some("demo-project"))
            .await
            .expect("list jobs");
        assert_eq!(jobs.len(), 2);
        assert!(
            jobs.iter()
                .all(|job| job.blueprint_name.as_deref() == Some("demo-project"))
        );
        let mut cluster_names: Vec<&str> = jobs.iter().map(|job| job.name.as_str()).collect();
        cluster_names.sort_unstable();
        assert_eq!(cluster_names, vec!["cluster-a", "cluster-b"]);
    }

    #[tokio::test]
    async fn list_jobs_filters_by_blueprint_with_cluster_filter() {
        let usecases = build_usecases_for_list_jobs_tests().await;
        seed_jobs_for_blueprint_filtering(&usecases).await;

        let jobs = usecases
            .list_jobs(Some("cluster-a"), Some("demo-project"))
            .await
            .expect("list jobs");
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].name, "cluster-a");
        assert_eq!(jobs[0].blueprint_name.as_deref(), Some("demo-project"));
    }

    #[tokio::test]
    async fn delete_cluster_rejects_running_jobs_without_force() {
        let remote_exec = Arc::new(DeleteClusterRemoteExec);
        let usecases = build_usecases_with_remote_exec(remote_exec).await;
        let host_id = usecases
            .clusters
            .upsert_host(&sample_host("cluster-a"))
            .await
            .expect("insert cluster");
        let job_id = usecases
            .jobs
            .insert_job(&sample_job(host_id, "run-a", None))
            .await
            .expect("insert job");

        let err = usecases
            .delete_cluster("cluster-a", false)
            .await
            .expect_err("delete should fail");

        assert_eq!(err.kind(), AppErrorKind::Conflict);
        assert_eq!(err.code(), codes::CONFLICT);
        assert!(err.message().contains("running jobs"));
        assert!(err.message().contains("--force"));
        assert!(
            usecases
                .clusters
                .get_by_name("cluster-a")
                .await
                .expect("cluster lookup")
                .is_some()
        );
        assert!(
            usecases
                .jobs
                .get_job_by_job_id(job_id)
                .await
                .expect("job lookup")
                .is_some()
        );
    }

    #[tokio::test]
    async fn delete_cluster_returns_not_found_for_unknown_cluster() {
        let remote_exec = Arc::new(DeleteClusterRemoteExec);
        let usecases = build_usecases_with_remote_exec(remote_exec).await;

        let err = usecases
            .delete_cluster("unknown-cluster", false)
            .await
            .expect_err("delete should fail");

        assert_eq!(err.kind(), AppErrorKind::InvalidArgument);
        assert_eq!(err.code(), codes::NOT_FOUND);
    }

    #[tokio::test]
    async fn delete_cluster_force_deletes_cluster_and_associated_jobs() {
        let remote_exec = Arc::new(DeleteClusterRemoteExec);
        let usecases = build_usecases_with_remote_exec(remote_exec).await;
        let host_id = usecases
            .clusters
            .upsert_host(&sample_host("cluster-a"))
            .await
            .expect("insert cluster");
        let job_id = usecases
            .jobs
            .insert_job(&sample_job(host_id, "run-a", None))
            .await
            .expect("insert job");

        let deleted = usecases
            .delete_cluster("cluster-a", true)
            .await
            .expect("delete should succeed");

        assert!(deleted);
        assert!(
            usecases
                .clusters
                .get_by_name("cluster-a")
                .await
                .expect("cluster lookup")
                .is_none()
        );
        assert!(
            usecases
                .jobs
                .get_job_by_job_id(job_id)
                .await
                .expect("job lookup")
                .is_none()
        );
    }

    #[tokio::test]
    async fn delete_cluster_promotes_last_added_when_default_is_removed() {
        let remote_exec = Arc::new(DeleteClusterRemoteExec);
        let usecases = build_usecases_with_remote_exec(remote_exec).await;

        let mut host_a = sample_host("cluster-a");
        host_a.is_default = true;
        let host_b = sample_host("cluster-b");
        let host_c = sample_host("cluster-c");

        usecases
            .clusters
            .upsert_host(&host_a)
            .await
            .expect("insert default cluster");
        usecases
            .clusters
            .upsert_host(&host_b)
            .await
            .expect("insert cluster-b");
        usecases
            .clusters
            .upsert_host(&host_c)
            .await
            .expect("insert cluster-c");

        let deleted = usecases
            .delete_cluster("cluster-a", true)
            .await
            .expect("delete should succeed");
        assert!(deleted);

        let hosts = usecases
            .clusters
            .list_hosts(None)
            .await
            .expect("list hosts after delete");
        assert_eq!(hosts.len(), 2);
        let default_hosts: Vec<_> = hosts.iter().filter(|host| host.is_default).collect();
        assert_eq!(default_hosts.len(), 1);
        assert_eq!(default_hosts[0].name, "cluster-c");
        assert!(hosts.iter().any(|host| host.name == "cluster-b"));
        assert!(hosts.iter().any(|host| host.name == "cluster-c"));
    }

    #[tokio::test]
    async fn connect_cluster_returns_not_found_for_unknown_cluster() {
        let usecases = build_usecases_for_list_jobs_tests().await;
        let stream = NoopStreamOutput;
        let mut mfa = NoopMfa::new();

        let err = usecases
            .connect_cluster("unknown-cluster", &stream, &mut mfa)
            .await
            .expect_err("must fail");
        assert_eq!(err.kind(), AppErrorKind::NotFound);
        assert_eq!(err.code(), codes::NOT_FOUND);
    }

    #[tokio::test]
    async fn connect_cluster_sends_keepalive_when_already_connected() {
        let remote_exec = Arc::new(ScriptedRemoteExec::new(
            Ok(ExecCapture {
                stdout: Vec::new(),
                stderr: Vec::new(),
                exit_code: 0,
            }),
            false,
            None,
        ));
        let usecases = build_usecases_with_remote_exec(remote_exec.clone()).await;
        usecases
            .clusters
            .upsert_host(&sample_localhost_host("cluster-a"))
            .await
            .expect("insert cluster");

        let stream = NoopStreamOutput;
        let mut mfa = NoopMfa::new();
        usecases
            .connect_cluster("cluster-a", &stream, &mut mfa)
            .await
            .expect("connect should succeed");

        assert_eq!(remote_exec.keepalive_calls(), 1);
        assert_eq!(remote_exec.ensure_connected_calls(), 0);
    }

    #[tokio::test]
    async fn connect_cluster_establishes_connection_when_disconnected() {
        let remote_exec = Arc::new(ScriptedRemoteExec::new(
            Ok(ExecCapture {
                stdout: Vec::new(),
                stderr: Vec::new(),
                exit_code: 0,
            }),
            true,
            None,
        ));
        let usecases = build_usecases_with_remote_exec(remote_exec.clone()).await;
        usecases
            .clusters
            .upsert_host(&sample_localhost_host("cluster-a"))
            .await
            .expect("insert cluster");

        let stream = NoopStreamOutput;
        let mut mfa = NoopMfa::new();
        usecases
            .connect_cluster("cluster-a", &stream, &mut mfa)
            .await
            .expect("connect should succeed");

        assert_eq!(remote_exec.keepalive_calls(), 0);
        assert_eq!(remote_exec.ensure_connected_calls(), 1);
    }

    const SAMPLE_PARTITIONS_OUTPUT_ANONYMIZED: &str = concat!(
        "PartitionName=alpha_interactive AllowGroups=ALL AllowAccounts=ALL AllowQos=ALL ",
        "AllocNodes=ALL Default=NO QoS=interac DefaultTime=00:15:00 DisableRootJobs=YES ",
        "ExclusiveUser=NO ExclusiveTopo=NO GraceTime=0 Hidden=NO MaxNodes=UNLIMITED ",
        "MaxTime=08:00:00 MinNodes=1 LLN=NO MaxCPUsPerNode=UNLIMITED ",
        "MaxCPUsPerSocket=UNLIMITED Nodes=c[1-10] PriorityJobFactor=1 PriorityTier=1 ",
        "RootOnly=NO ReqResv=NO OverSubscribe=NO OverTimeLimit=NONE PreemptMode=OFF State=UP ",
        "TotalCPUs=1920 TotalNodes=10 SelectTypeParameters=NONE JobDefaults=(null) ",
        "DefMemPerCPU=256 MaxMemPerNode=UNLIMITED ",
        "TRES=cpu=1920,mem=60000G,node=10,billing=15000000 ",
        "TRESBillingWeights=CPU=1000,Mem=250G\n",
        "PartitionName=beta_backfill AllowGroups=ALL AllowAccounts=ALL AllowQos=ALL ",
        "AllocNodes=ALL Default=NO QoS=N/A DefaultTime=00:15:00 DisableRootJobs=YES ",
        "ExclusiveUser=NO ExclusiveTopo=NO GraceTime=0 Hidden=NO MaxNodes=UNLIMITED ",
        "MaxTime=1-00:00:00 MinNodes=1 LLN=NO MaxCPUsPerNode=UNLIMITED ",
        "MaxCPUsPerSocket=UNLIMITED Nodes=g[1-5] PriorityJobFactor=12 PriorityTier=1 ",
        "RootOnly=NO ReqResv=NO OverSubscribe=NO OverTimeLimit=NONE PreemptMode=OFF State=UP ",
        "TotalCPUs=560 TotalNodes=5 SelectTypeParameters=NONE JobDefaults=(null) ",
        "DefMemPerCPU=256 MaxMemPerNode=UNLIMITED ",
        "TRES=cpu=560,mem=10000G,node=5,billing=500000,gres/gpu=80 ",
        "TRESBillingWeights=CPU=871.43,Mem=47.31G,GRES/gpu:h100=12200\n",
        "PartitionName=gamma_oversub AllowGroups=ALL AllowAccounts=ALL AllowQos=ALL ",
        "AllocNodes=ALL Default=NO QoS=N/A DefaultTime=00:15:00 DisableRootJobs=YES ",
        "ExclusiveUser=NO ExclusiveTopo=NO GraceTime=0 Hidden=NO MaxNodes=UNLIMITED ",
        "MaxTime=7-00:00:00 MinNodes=1 LLN=NO MaxCPUsPerNode=UNLIMITED ",
        "MaxCPUsPerSocket=UNLIMITED Nodes=o[1-3] PriorityJobFactor=4 PriorityTier=10 ",
        "RootOnly=NO ReqResv=NO OverSubscribe=FORCE:8 OverTimeLimit=NONE PreemptMode=OFF ",
        "State=UP TotalCPUs=132 TotalNodes=3 SelectTypeParameters=NONE JobDefaults=(null) ",
        "DefMemPerCPU=256 MaxMemPerNode=UNLIMITED ",
        "TRES=cpu=132,mem=552000M,node=3,billing=115028,gres/gpu=12,gres/gpu:t4=12 ",
        "TRESBillingWeights=CPU=871.43,Mem=47.31G,GRES/gpu:h100=12200\n"
    );

    #[tokio::test]
    async fn list_partitions_returns_partition_names() {
        let remote_exec: Arc<dyn RemoteExecPort> = Arc::new(ScriptedRemoteExec::new(
            Ok(ExecCapture {
                stdout: SAMPLE_PARTITIONS_OUTPUT_ANONYMIZED.as_bytes().to_vec(),
                stderr: Vec::new(),
                exit_code: 0,
            }),
            false,
            None,
        ));
        let usecases = build_usecases_with_remote_exec(remote_exec).await;
        usecases
            .clusters
            .upsert_host(&sample_localhost_host("cluster-a"))
            .await
            .expect("insert cluster");

        let partitions = usecases
            .list_partitions("cluster-a")
            .await
            .expect("list partitions");
        assert_eq!(
            partitions,
            vec![
                "alpha_interactive".to_string(),
                "beta_backfill".to_string(),
                "gamma_oversub".to_string(),
            ]
        );
    }

    #[tokio::test]
    async fn list_partitions_rejects_empty_cluster_name() {
        let usecases = build_usecases_for_list_jobs_tests().await;
        let err = usecases
            .list_partitions("   ")
            .await
            .expect_err("must fail");
        assert_eq!(err.kind(), AppErrorKind::InvalidArgument);
        assert_eq!(err.code(), codes::INVALID_ARGUMENT);
    }

    #[tokio::test]
    async fn list_partitions_returns_not_found_for_unknown_cluster() {
        let usecases = build_usecases_for_list_jobs_tests().await;
        let err = usecases
            .list_partitions("unknown-cluster")
            .await
            .expect_err("must fail");
        assert_eq!(err.kind(), AppErrorKind::NotFound);
        assert_eq!(err.code(), codes::NOT_FOUND);
    }

    #[tokio::test]
    async fn list_partitions_returns_remote_error_when_command_fails() {
        let remote_exec: Arc<dyn RemoteExecPort> = Arc::new(ScriptedRemoteExec::new(
            Ok(ExecCapture {
                stdout: Vec::new(),
                stderr: b"scontrol: command not found".to_vec(),
                exit_code: 127,
            }),
            false,
            None,
        ));
        let usecases = build_usecases_with_remote_exec(remote_exec).await;
        usecases
            .clusters
            .upsert_host(&sample_localhost_host("cluster-a"))
            .await
            .expect("insert cluster");

        let err = usecases
            .list_partitions("cluster-a")
            .await
            .expect_err("must fail");
        assert_eq!(err.kind(), AppErrorKind::Aborted);
        assert_eq!(err.code(), codes::REMOTE_ERROR);
        assert!(err.message().contains("exit code 127"));
        assert!(err.message().contains("command not found"));
    }

    const SAMPLE_ACCOUNTS_OUTPUT: &str =
        "def-zovoilis-ab_cpu\ndef-zovoilis-ab_gpu\nrrg-zovoilis_cpu\n";

    #[tokio::test]
    async fn list_accounts_returns_account_names() {
        let remote_exec: Arc<dyn RemoteExecPort> = Arc::new(ScriptedRemoteExec::new(
            Ok(ExecCapture {
                stdout: SAMPLE_ACCOUNTS_OUTPUT.as_bytes().to_vec(),
                stderr: Vec::new(),
                exit_code: 0,
            }),
            false,
            None,
        ));
        let usecases = build_usecases_with_remote_exec(remote_exec).await;
        usecases
            .clusters
            .upsert_host(&sample_localhost_host("cluster-a"))
            .await
            .expect("insert cluster");

        let accounts = usecases
            .list_accounts("cluster-a")
            .await
            .expect("list accounts");
        assert_eq!(
            accounts,
            vec![
                "def-zovoilis-ab_cpu".to_string(),
                "def-zovoilis-ab_gpu".to_string(),
                "rrg-zovoilis_cpu".to_string(),
            ]
        );
    }

    #[tokio::test]
    async fn list_accounts_returns_empty_for_empty_output() {
        let remote_exec: Arc<dyn RemoteExecPort> = Arc::new(ScriptedRemoteExec::new(
            Ok(ExecCapture {
                stdout: Vec::new(),
                stderr: Vec::new(),
                exit_code: 0,
            }),
            false,
            None,
        ));
        let usecases = build_usecases_with_remote_exec(remote_exec).await;
        usecases
            .clusters
            .upsert_host(&sample_localhost_host("cluster-a"))
            .await
            .expect("insert cluster");

        let accounts = usecases
            .list_accounts("cluster-a")
            .await
            .expect("list accounts");
        assert!(accounts.is_empty());
    }

    #[tokio::test]
    async fn list_accounts_rejects_empty_cluster_name() {
        let usecases = build_usecases_for_list_jobs_tests().await;
        let err = usecases.list_accounts("   ").await.expect_err("must fail");
        assert_eq!(err.kind(), AppErrorKind::InvalidArgument);
        assert_eq!(err.code(), codes::INVALID_ARGUMENT);
    }

    #[tokio::test]
    async fn list_accounts_returns_not_found_for_unknown_cluster() {
        let usecases = build_usecases_for_list_jobs_tests().await;
        let err = usecases
            .list_accounts("unknown-cluster")
            .await
            .expect_err("must fail");
        assert_eq!(err.kind(), AppErrorKind::NotFound);
        assert_eq!(err.code(), codes::NOT_FOUND);
    }

    #[tokio::test]
    async fn list_accounts_returns_remote_error_when_command_fails() {
        let remote_exec: Arc<dyn RemoteExecPort> = Arc::new(ScriptedRemoteExec::new(
            Ok(ExecCapture {
                stdout: Vec::new(),
                stderr: b"sshare: command not found".to_vec(),
                exit_code: 127,
            }),
            false,
            None,
        ));
        let usecases = build_usecases_with_remote_exec(remote_exec).await;
        usecases
            .clusters
            .upsert_host(&sample_localhost_host("cluster-a"))
            .await
            .expect("insert cluster");

        let err = usecases
            .list_accounts("cluster-a")
            .await
            .expect_err("must fail");
        assert_eq!(err.kind(), AppErrorKind::Aborted);
        assert_eq!(err.code(), codes::REMOTE_ERROR);
        assert!(err.message().contains("exit code 127"));
        assert!(err.message().contains("command not found"));
    }

    #[test]
    fn retrieve_local_target_strips_prefix_for_relative_file() {
        // Ensures relative file paths drop remote prefixes and keep only the final filename.
        let base = Path::new("local");
        let target = resolve_retrieve_local_target(
            "results/result.txt",
            "/remote/results/result.txt",
            base,
            false,
        )
        .expect("target");
        assert_eq!(target, base.join("result.txt"));
    }

    #[test]
    fn retrieve_local_target_strips_prefix_for_relative_dir() {
        // Ensures relative directory paths map to the local base plus the final directory name.
        let base = Path::new("local");
        let target = resolve_retrieve_local_target(
            "results/nested/out",
            "/remote/results/nested/out",
            base,
            false,
        )
        .expect("target");
        assert_eq!(target, base.join("out"));
    }

    #[test]
    fn retrieve_local_target_uses_basename_for_absolute_path() {
        // Ensures absolute paths use the basename under the local base.
        let base = Path::new("local");
        let target = resolve_retrieve_local_target(
            "/remote/results/output.log",
            "/remote/results/output.log",
            base,
            true,
        )
        .expect("target");
        assert_eq!(target, base.join("output.log"));
    }

    fn sample_ssh_config_for_validation() -> SshConfig {
        build_ssh_config(
            Some("test-session".to_string()),
            "alice".to_string(),
            &Address::Hostname("cluster.example.com".to_string()),
            "127.0.0.1:22".parse().expect("socket addr"),
            None,
        )
    }

    fn ok_capture(exit_code: i32, stdout: &str) -> AppResult<ExecCapture> {
        Ok(ExecCapture {
            stdout: stdout.as_bytes().to_vec(),
            stderr: Vec::new(),
            exit_code,
        })
    }

    #[tokio::test]
    async fn validate_and_prepare_default_base_path_creates_only_requested_directory() {
        let base_path = "/home/alice/runs";
        let parent = "/home/alice";
        let base_path_escaped = shell::sh_escape(base_path);
        let parent_escaped = shell::sh_escape(parent);
        let remote_exec = SequencedRemoteExec::new(vec![
            (format!("test -d {base_path_escaped}"), ok_capture(1, "")),
            (format!("test -e {base_path_escaped}"), ok_capture(1, "")),
            (format!("test -d {parent_escaped}"), ok_capture(0, "")),
            (
                format!("test -w {parent_escaped} && test -x {parent_escaped}"),
                ok_capture(0, ""),
            ),
            (format!("mkdir -- {base_path_escaped}"), ok_capture(0, "")),
            (
                format!("cd -- {base_path_escaped} && pwd -P"),
                ok_capture(0, "/home/alice/runs\n"),
            ),
        ]);

        let config = sample_ssh_config_for_validation();
        validate_and_prepare_default_base_path(&remote_exec, &config, base_path)
            .await
            .expect("base path should be prepared");
        assert!(
            remote_exec
                .captures
                .lock()
                .expect("captures lock")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn validate_and_prepare_default_base_path_rejects_missing_parent() {
        let base_path = "/new/root/runs";
        let parent = "/new/root";
        let base_path_escaped = shell::sh_escape(base_path);
        let parent_escaped = shell::sh_escape(parent);
        let remote_exec = SequencedRemoteExec::new(vec![
            (format!("test -d {base_path_escaped}"), ok_capture(1, "")),
            (format!("test -e {base_path_escaped}"), ok_capture(1, "")),
            (format!("test -d {parent_escaped}"), ok_capture(1, "")),
        ]);

        let config = sample_ssh_config_for_validation();
        let err = validate_and_prepare_default_base_path(&remote_exec, &config, base_path)
            .await
            .expect_err("must reject missing parent");
        assert_eq!(err.code(), codes::INVALID_ARGUMENT);
        assert!(
            err.message().contains("only one level can be created"),
            "unexpected error: {}",
            err.message()
        );
    }

    #[tokio::test]
    async fn validate_and_prepare_default_base_path_rejects_insufficient_parent_permissions() {
        let base_path = "/home/alice/runs";
        let parent = "/home/alice";
        let base_path_escaped = shell::sh_escape(base_path);
        let parent_escaped = shell::sh_escape(parent);
        let remote_exec = SequencedRemoteExec::new(vec![
            (format!("test -d {base_path_escaped}"), ok_capture(1, "")),
            (format!("test -e {base_path_escaped}"), ok_capture(1, "")),
            (format!("test -d {parent_escaped}"), ok_capture(0, "")),
            (
                format!("test -w {parent_escaped} && test -x {parent_escaped}"),
                ok_capture(1, ""),
            ),
        ]);

        let config = sample_ssh_config_for_validation();
        let err = validate_and_prepare_default_base_path(&remote_exec, &config, base_path)
            .await
            .expect_err("must reject insufficient parent permissions");
        assert_eq!(err.code(), codes::INVALID_ARGUMENT);
        assert!(err.message().contains("insufficient permissions"));
    }

    #[tokio::test]
    async fn validate_and_prepare_default_base_path_rejects_existing_non_directory() {
        let base_path = "/home/alice/runs";
        let base_path_escaped = shell::sh_escape(base_path);
        let remote_exec = SequencedRemoteExec::new(vec![
            (format!("test -d {base_path_escaped}"), ok_capture(1, "")),
            (format!("test -e {base_path_escaped}"), ok_capture(0, "")),
        ]);

        let config = sample_ssh_config_for_validation();
        let err = validate_and_prepare_default_base_path(&remote_exec, &config, base_path)
            .await
            .expect_err("must reject existing non-directory");
        assert_eq!(err.code(), codes::INVALID_ARGUMENT);
        assert!(err.message().contains("exists but is not a directory"));
    }

    #[test]
    fn resolve_default_base_path_expands_home() {
        // Verifies tilde expansion for "~" and "~/runs" into the provided home directory.
        let home = "/home/alice";
        let resolved = resolve_default_base_path(Some("~".to_string()), home).unwrap();
        assert_eq!(resolved, Some(home.to_string()));

        let expected = PathBuf::from(home)
            .join("runs")
            .to_string_lossy()
            .into_owned();
        let resolved = resolve_default_base_path(Some("~/runs".to_string()), home).unwrap();
        assert_eq!(resolved, Some(expected));
    }

    #[test]
    fn resolve_default_base_path_defaults_to_home_runs() {
        // Checks that missing input defaults to "<home>/runs".
        let home = "/home/alice";
        let expected = PathBuf::from(home)
            .join("runs")
            .to_string_lossy()
            .into_owned();
        let resolved = resolve_default_base_path(None, home).unwrap();
        assert_eq!(resolved, Some(expected));
    }

    #[test]
    fn resolve_default_base_path_rejects_tilde_prefix() {
        // Rejects non-standard tilde prefixes like "~other".
        let err = resolve_default_base_path(Some("~other".to_string()), "/home").unwrap_err();
        assert_eq!(err.code(), codes::INVALID_ARGUMENT);
    }

    #[test]
    fn normalize_default_base_path_rejects_relative() {
        // Disallows relative default base paths to avoid ambiguous storage roots.
        let err = normalize_default_base_path(Some("relative/path".to_string())).unwrap_err();
        assert_eq!(err.code(), codes::INVALID_ARGUMENT);
    }

    #[test]
    fn resolve_default_scratch_directory_expands_home_and_preserves_none() {
        let home = "/home/alice";
        let resolved = resolve_default_scratch_directory(Some("~/scratch".to_string()), home)
            .expect("scratch path should resolve");
        assert_eq!(resolved, Some("/home/alice/scratch".to_string()));

        let resolved = resolve_default_scratch_directory(None, home).expect("none should pass");
        assert_eq!(resolved, None);
    }

    #[test]
    fn resolve_default_scratch_directory_rejects_tilde_prefix() {
        let err =
            resolve_default_scratch_directory(Some("~other".to_string()), "/home").unwrap_err();
        assert_eq!(err.code(), codes::INVALID_ARGUMENT);
    }

    #[test]
    fn normalize_default_scratch_directory_rejects_relative() {
        let err =
            normalize_default_scratch_directory(Some("relative/path".to_string())).unwrap_err();
        assert_eq!(err.code(), codes::INVALID_ARGUMENT);
    }

    #[test]
    fn normalize_blueprint_filter_accepts_none_and_trims() {
        assert_eq!(normalize_blueprint_filter(None).unwrap(), None);
        assert_eq!(
            normalize_blueprint_filter(Some("  demo-project  ")).unwrap(),
            Some("demo-project".to_string())
        );
    }

    #[test]
    fn normalize_blueprint_filter_rejects_empty_value() {
        let err = normalize_blueprint_filter(Some("   ")).unwrap_err();
        assert_eq!(err.code(), codes::INVALID_ARGUMENT);
    }
}
