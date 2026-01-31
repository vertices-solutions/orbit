// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

use std::collections::HashSet;
use proto::{ListClustersUnitResponse, ListJobsUnitResponse};

use crate::app::commands::*;
use crate::app::errors::{
    error_type_for_remote_code, AppError, AppResult, ErrorContext, ErrorType,
};
use crate::app::ports::{StreamKind, StreamOutputPort};
use crate::app::services::{
    default_base_path_from_home, validate_default_base_path, AddClusterResolver, PathResolver,
    SbatchSelector,
};
use crate::app::AppContext;
use crate::errors::format_server_error;
use crate::format::{cluster_host_string};

pub async fn handle_ping(ctx: &AppContext, _cmd: PingCommand) -> AppResult<CommandResult> {
    ctx.orbitd.ping().await?;
    Ok(CommandResult::Pong {
        message: "pong".to_string(),
    })
}

pub async fn handle_job_list(ctx: &AppContext, cmd: ListJobsCommand) -> AppResult<CommandResult> {
    let jobs = ctx.orbitd.list_jobs(cmd.cluster).await?;
    Ok(CommandResult::JobList { jobs })
}

pub async fn handle_job_get(ctx: &AppContext, cmd: JobGetCommand) -> AppResult<CommandResult> {
    let jobs = ctx.orbitd.list_jobs(cmd.cluster.clone()).await?;
    let matches: Vec<&ListJobsUnitResponse> = jobs
        .iter()
        .filter(|job| job.job_id == cmd.job_id)
        .collect();
    match matches.as_slice() {
        [] => {
            if let Some(cluster) = cmd.cluster.as_deref() {
                return Err(AppError::job_not_found(format!(
                    "job id {} not found in cluster '{}'",
                    cmd.job_id, cluster
                )));
            }
            Err(AppError::job_not_found(format!(
                "job id {} not found",
                cmd.job_id
            )))
        }
        [job] => Ok(CommandResult::JobDetails { job: (*job).clone() }),
        _ => {
            if cmd.cluster.is_some() {
                return Err(AppError::invalid_argument(format!(
                    "multiple jobs matched job id {}",
                    cmd.job_id
                )));
            }
            Err(AppError::invalid_argument(format!(
                "job id {} matched multiple clusters; use --cluster",
                cmd.job_id
            )))
        }
    }
}

pub async fn handle_job_submit(
    ctx: &AppContext,
    cmd: SubmitJobCommand,
) -> AppResult<CommandResult> {
    let clusters = ctx.orbitd.list_clusters("").await?;
    let cluster = clusters
        .iter()
        .find(|cluster| cluster.name == cmd.name)
        .cloned()
        .ok_or_else(|| {
            AppError::cluster_not_found(format!(
                "cluster '{}' not found; use 'cluster add' to create it",
                cmd.name
            ))
        })?;

    validate_cluster_live(ctx, &cluster).await?;

    let resolver = PathResolver::new(ctx.fs.as_ref());
    let resolved_local_path = resolver.resolve_local(&cmd.local_path)?;
    let sbatch_selector = SbatchSelector::new(ctx.fs.as_ref(), ctx.interaction.as_ref(), ctx.ui_mode);
    let sbatchscript = sbatch_selector
        .select(&resolved_local_path, cmd.sbatchscript.as_deref())
        .await?;

    let resolved_local_path_display = resolved_local_path.display().to_string();
    ctx.output
        .success(&format!("Selected sbatch script: {sbatchscript}"))
        .await?;
    ctx.output
        .success(&format!("Local path: {resolved_local_path_display}"))
        .await?;

    let mut stream_output = ctx.output.stream_output(StreamKind::Submit);
    let capture = ctx
        .orbitd
        .submit(
            cmd.name.clone(),
            resolved_local_path_display.clone(),
            cmd.remote_path,
            cmd.new_directory,
            cmd.force,
            sbatchscript.clone(),
            cmd.filters,
            &mut *stream_output,
            ctx.interaction.as_ref(),
        )
        .await?;

    if capture.exit_code.unwrap_or(0) != 0 {
        return Err(submit_error(&capture));
    }

    Ok(CommandResult::JobSubmit {
        cluster: cmd.name,
        local_path: resolved_local_path_display,
        sbatchscript,
        capture,
    })
}

pub async fn handle_job_logs(ctx: &AppContext, cmd: JobLogsCommand) -> AppResult<CommandResult> {
    let mut stream_output = ctx.output.stream_output(StreamKind::Generic);
    let capture = ctx
        .orbitd
        .job_logs(
            cmd.job_id,
            cmd.err,
            &mut *stream_output,
            ctx.interaction.as_ref(),
        )
        .await?;
    if capture.exit_code.unwrap_or(0) != 0 {
        return Err(stream_error(
            &capture,
            ErrorContext::Job,
            "command failed",
        ));
    }
    Ok(CommandResult::JobLogs { capture })
}

pub async fn handle_job_cancel(
    ctx: &AppContext,
    cmd: JobCancelCommand,
) -> AppResult<CommandResult> {
    if !cmd.yes {
        let confirmed = ctx
            .interaction
            .confirm(
                &format!("Cancel job {}? (yes/no): ", cmd.job_id),
                "Type yes to confirm, no to cancel.",
            )
            .await?;
        if !confirmed {
            return Ok(CommandResult::Message {
                message: "Cancel canceled.".to_string(),
            });
        }
    }

    let mut stream_output = ctx.output.stream_output(StreamKind::Generic);
    let capture = ctx
        .orbitd
        .job_cancel(cmd.job_id, &mut *stream_output, ctx.interaction.as_ref())
        .await?;
    if capture.exit_code.unwrap_or(0) != 0 {
        return Err(stream_error(
            &capture,
            ErrorContext::Job,
            "command failed",
        ));
    }

    Ok(CommandResult::JobCancel {
        job_id: cmd.job_id,
        capture,
    })
}

pub async fn handle_job_cleanup(
    ctx: &AppContext,
    cmd: JobCleanupCommand,
) -> AppResult<CommandResult> {
    if !cmd.yes {
        ctx.output
            .info(&format!(
                "WARNING:\nThis will delete the remote directory for job {}.",
                cmd.job_id
            ))
            .await?;
        if cmd.force {
            ctx.output
                .info("The job will be canceled before cleanup.")
                .await?;
        } else {
            ctx.output
                .info("If the job is still running, pass --force to cancel it.")
                .await?;
        }
        if cmd.full {
            ctx.output
                .info("The job record will be deleted from the local database.")
                .await?;
        }
        ctx.output
            .info("This action cannot be undone.")
            .await?;
        let confirmed = ctx
            .interaction
            .confirm(
                "Continue with cleanup? (yes/no): ",
                "Type yes to confirm, no to cancel.",
            )
            .await?;
        if !confirmed {
            return Ok(CommandResult::Message {
                message: "Cleanup canceled.".to_string(),
            });
        }
    }

    let mut stream_output = ctx.output.stream_output(StreamKind::Generic);
    let capture = ctx
        .orbitd
        .job_cleanup(
            cmd.job_id,
            cmd.force,
            cmd.full,
            &mut *stream_output,
            ctx.interaction.as_ref(),
        )
        .await?;
    if capture.exit_code.unwrap_or(0) != 0 {
        return Err(stream_error(
            &capture,
            ErrorContext::Job,
            "command failed",
        ));
    }

    Ok(CommandResult::JobCleanup {
        job_id: cmd.job_id,
        force: cmd.force,
        full: cmd.full,
        capture,
    })
}

pub async fn handle_job_ls(ctx: &AppContext, cmd: JobLsCommand) -> AppResult<CommandResult> {
    let mut stream_output = ctx.output.stream_output(StreamKind::Generic);
    let capture = ctx
        .orbitd
        .ls(
            cmd.cluster.unwrap_or_default(),
            Some(cmd.job_id),
            cmd.path,
            &mut *stream_output,
            ctx.interaction.as_ref(),
        )
        .await?;
    if capture.exit_code.unwrap_or(0) != 0 {
        return Err(stream_error(
            &capture,
            ErrorContext::Job,
            "command failed",
        ));
    }
    Ok(CommandResult::JobLs { capture })
}

pub async fn handle_job_retrieve(
    ctx: &AppContext,
    cmd: JobRetrieveCommand,
) -> AppResult<CommandResult> {
    let mut stream_output = ctx.output.stream_output(StreamKind::Generic);
    let (local_target, capture) = ctx
        .orbitd
        .job_retrieve(
            cmd.job_id,
            cmd.path.clone(),
            cmd.output,
            cmd.overwrite,
            cmd.force,
            &mut *stream_output,
            ctx.interaction.as_ref(),
        )
        .await?;
    if capture.exit_code.unwrap_or(0) != 0 {
        return Err(stream_error(
            &capture,
            ErrorContext::Job,
            "command failed",
        ));
    }
    Ok(CommandResult::JobRetrieve {
        job_id: cmd.job_id,
        path: cmd.path,
        output: local_target,
        capture,
    })
}

pub async fn handle_cluster_list(
    ctx: &AppContext,
    _cmd: ListClustersCommand,
) -> AppResult<CommandResult> {
    let clusters = ctx.orbitd.list_clusters("").await?;
    Ok(CommandResult::ClusterList { clusters })
}

pub async fn handle_cluster_get(
    ctx: &AppContext,
    cmd: ClusterGetCommand,
) -> AppResult<CommandResult> {
    let clusters = ctx.orbitd.list_clusters("").await?;
    let cluster = clusters
        .iter()
        .find(|cluster| cluster.name == cmd.name)
        .cloned()
        .ok_or_else(|| AppError::cluster_not_found(format!("cluster '{}' not found", cmd.name)))?;
    Ok(CommandResult::ClusterDetails { cluster })
}

pub async fn handle_cluster_ls(
    ctx: &AppContext,
    cmd: ClusterLsCommand,
) -> AppResult<CommandResult> {
    let mut stream_output = ctx.output.stream_output(StreamKind::Generic);
    let capture = ctx
        .orbitd
        .ls(
            cmd.name,
            None,
            cmd.path,
            &mut *stream_output,
            ctx.interaction.as_ref(),
        )
        .await?;
    if capture.exit_code.unwrap_or(0) != 0 {
        return Err(stream_error(
            &capture,
            ErrorContext::Cluster,
            "command failed",
        ));
    }
    Ok(CommandResult::ClusterLs { capture })
}

pub async fn handle_cluster_add(
    ctx: &AppContext,
    cmd: AddClusterCommand,
) -> AppResult<CommandResult> {
    ctx.output.info("Adding new cluster...").await?;
    let clusters = ctx.orbitd.list_clusters("").await?;
    let existing_names = clusters
        .iter()
        .map(|cluster| cluster.name.clone())
        .collect::<HashSet<_>>();
    let resolver = AddClusterResolver::new(
        ctx.interaction.as_ref(),
        ctx.fs.as_ref(),
        ctx.network.as_ref(),
        ctx.output.as_ref(),
        ctx.ui_mode,
    );
    let mut resolved = resolver.resolve(cmd, &existing_names).await?;
    if let Some(host) = resolved.hostname.as_deref().or(resolved.ip.as_deref()) {
        if let Some(existing) = clusters.iter().find(|cluster| {
            cluster.username == resolved.username
                && cluster_host_string(cluster) == host
                && cluster.port == resolved.port as i32
        }) {
            return Err(AppError::conflict(format!(
                "another cluster with name '{}' is already using {}:{}:{}; use 'cluster set' to update it.",
                existing.name,
                resolved.username,
                host,
                resolved.port
            )));
        }
    }

    let mut needs_base_path_prompt = resolved.default_base_path.is_none();
    if let Some(ref value) = resolved.default_base_path {
        if let Err(err) = validate_default_base_path(value) {
            if !ctx.ui_mode.is_interactive() {
                return Err(err);
            }
            ctx.output
                .warn(&format!(
                    "Default base path '{}' is invalid: {}",
                    value, err.message
                ))
                .await?;
            needs_base_path_prompt = true;
        }
    }

    if needs_base_path_prompt {
        let home_dir = ctx
            .orbitd
            .resolve_home_dir(
                Some(resolved.name.clone()),
                resolved.username.clone(),
                resolved.hostname.clone(),
                resolved.ip.clone(),
                Some(resolved.identity_path.clone()),
                resolved.port,
                ctx.interaction.as_ref(),
            )
            .await?;
        loop {
            let default_path = default_base_path_from_home(&home_dir);
            let base_path = ctx
                .interaction
                .prompt_line_with_default(
                    "Default base path: ",
                    "Remote base folder for projects.",
                    &default_path,
                )
                .await?;
            match validate_default_base_path(&base_path) {
                Ok(()) => {
                    resolved.default_base_path = Some(base_path);
                    break;
                }
                Err(err) => {
                    ctx.output
                        .warn(&format!(
                            "Default base path '{}' is invalid: {}",
                            base_path, err.message
                        ))
                        .await?;
                }
            }
        }
    }

    let mut stream_output = ctx.output.stream_output(StreamKind::Generic);
    let capture = ctx
        .orbitd
        .add_cluster(
            resolved.name.clone(),
            resolved.username.clone(),
            resolved.hostname.clone(),
            resolved.ip.clone(),
            Some(resolved.identity_path.clone()),
            resolved.port,
            resolved.default_base_path.clone(),
            &mut *stream_output,
            ctx.interaction.as_ref(),
        )
        .await?;
    if capture.exit_code.unwrap_or(0) != 0 {
        return Err(stream_error(
            &capture,
            ErrorContext::Cluster,
            "command failed",
        ));
    }

    Ok(CommandResult::ClusterAdd {
        name: resolved.name,
        username: resolved.username,
        hostname: resolved.hostname,
        ip: resolved.ip,
        port: resolved.port,
        identity_path: resolved.identity_path,
        default_base_path: resolved.default_base_path,
    })
}

pub async fn handle_cluster_set(
    ctx: &AppContext,
    cmd: SetClusterCommand,
) -> AppResult<CommandResult> {
    let mut updated_fields = Vec::new();
    if let Some(value) = cmd.host.as_deref() {
        updated_fields.push(("host".to_string(), value.to_string()));
    }
    if let Some(value) = cmd.username.as_deref() {
        updated_fields.push(("username".to_string(), value.to_string()));
    }
    if let Some(value) = cmd.port {
        updated_fields.push(("port".to_string(), value.to_string()));
    }
    if let Some(value) = cmd.identity_path.as_deref() {
        updated_fields.push(("identity_path".to_string(), value.to_string()));
    }
    if let Some(value) = cmd.default_base_path.as_deref() {
        updated_fields.push(("default_base_path".to_string(), value.to_string()));
    }

    let clusters = ctx.orbitd.list_clusters("").await?;
    let cluster = clusters
        .iter()
        .find(|cluster| cluster.name == cmd.name)
        .cloned()
        .ok_or_else(|| {
            AppError::cluster_not_found(format!(
                "cluster '{}' not found; use 'cluster add' to create it",
                cmd.name
            ))
        })?;

    if cmd.host.is_none() && cluster.host.is_none() {
        return Err(AppError::invalid_argument(format!(
            "cluster '{}' has no address; pass --host to update it",
            cmd.name
        )));
    }

    let mut stream_output = ctx.output.stream_output(StreamKind::Generic);
    let capture = ctx
        .orbitd
        .set_cluster(
            cluster.name.clone(),
            cmd.host.clone(),
            cmd.username.clone(),
            cmd.identity_path.clone(),
            cmd.port,
            cmd.default_base_path.clone(),
            &mut *stream_output,
            ctx.interaction.as_ref(),
        )
        .await?;
    if capture.exit_code.unwrap_or(0) != 0 {
        return Err(stream_error(
            &capture,
            ErrorContext::Cluster,
            "command failed",
        ));
    }

    Ok(CommandResult::ClusterSet {
        name: cluster.name,
        updated_fields,
    })
}

pub async fn handle_cluster_delete(
    ctx: &AppContext,
    cmd: DeleteClusterCommand,
) -> AppResult<CommandResult> {
    if !cmd.yes {
        ctx.output
            .info(&format!(
                "WARNING:\nThis will delete cluster '{}' and all its job records from the local database.",
                cmd.name
            ))
            .await?;
        ctx.output
            .info("Any active SSH sessions for this cluster will be closed.")
            .await?;
        ctx.output.info("This action cannot be undone.").await?;
        let confirmed = ctx
            .interaction
            .confirm(
                "Continue with delete? (yes/no): ",
                "Type yes to confirm, no to cancel.",
            )
            .await?;
        if !confirmed {
            return Ok(CommandResult::Message {
                message: "Delete canceled.".to_string(),
            });
        }
    }

    let deleted = ctx.orbitd.delete_cluster(&cmd.name).await?;
    if !deleted {
        return Err(AppError::cluster_not_found(format!(
            "cluster name '{}' is not known",
            cmd.name
        )));
    }

    Ok(CommandResult::ClusterDelete { name: cmd.name })
}

fn bytes_to_string(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).to_string()
}

fn message_from_bytes(bytes: &[u8]) -> Option<String> {
    let text = bytes_to_string(bytes);
    let trimmed = text.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn stream_error(capture: &StreamCapture, context: ErrorContext, default: &str) -> AppError {
    let exit_code = capture.exit_code.unwrap_or(1);
    if let Some(code) = capture.error_code.as_deref() {
        let kind = error_type_for_remote_code(code, context);
        let message = format_server_error(code);
        return AppError::with_exit_code(kind, message, exit_code);
    }
    if let Some(message) = message_from_bytes(&capture.stderr) {
        return AppError::with_exit_code(ErrorType::RemoteError, message, exit_code);
    }
    AppError::with_exit_code(ErrorType::RemoteError, default, exit_code)
}

fn submit_error(capture: &SubmitCapture) -> AppError {
    let exit_code = capture.exit_code.unwrap_or(1);
    if let Some(detail) = capture.detail.as_deref() {
        let trimmed = detail.trim();
        if !trimmed.is_empty() {
            return AppError::with_exit_code(
                ErrorType::RemoteError,
                format_server_error(trimmed),
                exit_code,
            );
        }
    }
    if let Some(message) = message_from_bytes(&capture.stderr) {
        return AppError::with_exit_code(ErrorType::RemoteError, message, exit_code);
    }
    if let Some(code) = capture.error_code.as_deref() {
        let kind = error_type_for_remote_code(code, ErrorContext::Job);
        let message = format_server_error(code);
        return AppError::with_exit_code(kind, message, exit_code);
    }
    AppError::with_exit_code(ErrorType::RemoteError, "submission failed", exit_code)
}

async fn validate_cluster_live(ctx: &AppContext, cluster: &ListClustersUnitResponse) -> AppResult<()> {
    let host = match cluster.host.as_ref() {
        Some(proto::list_clusters_unit_response::Host::Hostname(value)) => value.as_str(),
        Some(proto::list_clusters_unit_response::Host::Ipaddr(value)) => value.as_str(),
        None => {
            return Err(AppError::invalid_argument(format!(
                "cluster '{}' has no configured host",
                cluster.name
            )))
        }
    };
    let port = u16::try_from(cluster.port).map_err(|_| {
        AppError::invalid_argument(format!("cluster '{}' has invalid port", cluster.name))
    })?;
    ctx.network.check_reachable(host, port)?;

    let mut stream_output = SilentStreamOutput::default();
    let capture = ctx
        .orbitd
        .ls(
            cluster.name.clone(),
            None,
            None,
            &mut stream_output,
            ctx.interaction.as_ref(),
        )
        .await?;

    match capture.exit_code {
        Some(0) => {
            if ctx.ui_mode.is_interactive() {
                ctx.output
                    .success(&format!("{} live", cluster.name))
                    .await?;
            }
            Ok(())
        }
        Some(code) => {
            let detail = if capture.stderr.is_empty() {
                format!("exit code {code}")
            } else {
                String::from_utf8_lossy(&capture.stderr).to_string()
            };
            Err(AppError::network_error(format!(
                "cluster '{}' did not respond to checks: {}",
                cluster.name,
                detail.trim()
            )))
        }
        None => Err(AppError::network_error(format!(
            "cluster '{}' did not respond to checks",
            cluster.name
        ))),
    }
}

#[derive(Default)]
struct SilentStreamOutput {
    stream: StreamCapture,
    submit: SubmitCapture,
}

#[tonic::async_trait]
impl StreamOutputPort for SilentStreamOutput {
    async fn on_stdout(&mut self, bytes: &[u8]) -> AppResult<()> {
        self.stream.stdout.extend_from_slice(bytes);
        Ok(())
    }

    async fn on_stderr(&mut self, bytes: &[u8]) -> AppResult<()> {
        self.stream.stderr.extend_from_slice(bytes);
        Ok(())
    }

    async fn on_exit_code(&mut self, code: i32) -> AppResult<()> {
        self.stream.exit_code = Some(code);
        Ok(())
    }

    async fn on_error(&mut self, code: &str) -> AppResult<()> {
        self.stream.error_code = Some(code.to_string());
        Ok(())
    }

    async fn on_submit_status(&mut self, _status: &proto::SubmitStatus) -> AppResult<()> {
        Ok(())
    }

    async fn on_submit_result(&mut self, _result: &proto::SubmitResult) -> AppResult<()> {
        Ok(())
    }

    fn take_stream_capture(&mut self) -> StreamCapture {
        std::mem::take(&mut self.stream)
    }

    fn take_submit_capture(&mut self) -> SubmitCapture {
        std::mem::take(&mut self.submit)
    }
}
