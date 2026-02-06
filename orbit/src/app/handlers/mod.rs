// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

use proto::{ListClustersUnitResponse, ListJobsUnitResponse};
use serde_json::json;
use std::collections::HashSet;
use std::path::PathBuf;

use crate::app::AppContext;
use crate::app::commands::*;
use crate::app::errors::{
    AppError, AppResult, ErrorContext, ErrorType, error_type_for_remote_code, format_server_error,
};
use crate::app::ports::{StreamKind, StreamOutputPort};
use crate::app::services::{
    AddClusterResolver, PathResolver, ProjectRuleSet, SbatchSelector,
    build_default_orbitfile_contents, check_registered_project, default_base_path_from_home,
    discover_project_from_submit_root, load_project_from_root, local_validate_default_base_path,
    merge_submit_filters, resolve_orbitfile_sbatch_script, resolve_template_values,
    sanitize_project_name, template_config_from_json, upsert_orbitfile_project_name,
    validate_project_name,
};

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
    let matches: Vec<&ListJobsUnitResponse> =
        jobs.iter().filter(|job| job.job_id == cmd.job_id).collect();
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
        [job] => Ok(CommandResult::JobDetails {
            job: (*job).clone(),
        }),
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
    let clusters = ctx.orbitd.list_clusters("", true).await?;
    let cluster = clusters
        .iter()
        .find(|cluster| cluster.name == cmd.cluster)
        .cloned()
        .ok_or_else(|| {
            AppError::cluster_not_found(format!(
                "cluster '{}' not found; use 'cluster add' to create it",
                cmd.cluster
            ))
        })?;

    validate_cluster_live(ctx, &cluster).await?;

    let resolver = PathResolver::new(ctx.fs.as_ref());
    let resolved_local_path = resolver.resolve_local(&cmd.local_path)?;
    let discovered_project =
        discover_project_from_submit_root(ctx.fs.as_ref(), &resolved_local_path)?;
    let sbatch_selector =
        SbatchSelector::new(ctx.fs.as_ref(), ctx.interaction.as_ref(), ctx.ui_mode);
    let sbatchscript = if let Some(explicit) = cmd.sbatchscript.as_deref() {
        sbatch_selector
            .select(&resolved_local_path, Some(explicit))
            .await?
    } else if let Some(project) = discovered_project.as_ref() {
        if let Some(configured) = project.submit_sbatch_script.as_deref() {
            resolve_orbitfile_sbatch_script(
                ctx.fs.as_ref(),
                &project.root,
                &resolved_local_path,
                configured,
            )?
        } else {
            sbatch_selector.select(&resolved_local_path, None).await?
        }
    } else {
        sbatch_selector.select(&resolved_local_path, None).await?
    };

    let mut filters = cmd.filters.clone();
    let mut project_name = None;
    let mut default_retrieve_path = None;
    let mut template_values_json = None;
    if let Some(project) = discovered_project {
        validate_project_name(&project.name)?;
        filters = merge_submit_filters(filters, &project.rules);
        project_name = Some(project.name.clone());
        default_retrieve_path = project.default_retrieve_path.clone();
        let project_root = project.root.display().to_string();
        if let Some(template) = project.template.as_ref() {
            let values = resolve_template_values(
                template,
                cmd.template_preset.as_deref(),
                &cmd.template_fields,
                cmd.fill_defaults,
                &project.root,
                ctx.fs.as_ref(),
                ctx.interaction.as_ref(),
                ctx.output.as_ref(),
                ctx.ui_mode,
                true,
            )
            .await?;
            let json = serde_json::to_string(&values).map_err(|err| {
                AppError::internal_error(format!("failed to serialize templates: {err}"))
            })?;
            template_values_json = Some(json);
        } else if cmd.template_preset.is_some() || !cmd.template_fields.is_empty() {
            return Err(AppError::invalid_argument(
                "template values provided but Orbitfile has no [template] section",
            ));
        }
        let _ = ctx
            .orbitd
            .upsert_project(&project.name, &project_root)
            .await?;
    } else if cmd.template_preset.is_some() || !cmd.template_fields.is_empty() {
        return Err(AppError::invalid_argument(
            "template values require an Orbitfile with a [template] section",
        ));
    }

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
        .submit_job(
            cmd.cluster.clone(),
            resolved_local_path_display.clone(),
            cmd.remote_path,
            cmd.new_directory,
            cmd.force,
            sbatchscript.clone(),
            filters,
            project_name,
            default_retrieve_path,
            template_values_json,
            &mut *stream_output,
            ctx.interaction.as_ref(),
        )
        .await?;

    if capture.exit_code.unwrap_or(0) != 0 {
        return Err(submit_error(&capture));
    }

    Ok(CommandResult::JobSubmit {
        cluster: cmd.cluster,
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
        return Err(stream_error(&capture, ErrorContext::Job, "command failed"));
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
        return Err(stream_error(&capture, ErrorContext::Job, "command failed"));
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
        ctx.output.info("This action cannot be undone.").await?;
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
        return Err(stream_error(&capture, ErrorContext::Job, "command failed"));
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
        return Err(stream_error(&capture, ErrorContext::Job, "command failed"));
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
        return Err(stream_error(&capture, ErrorContext::Job, "command failed"));
    }
    Ok(CommandResult::JobRetrieve {
        job_id: cmd.job_id,
        path: cmd.path.unwrap_or_default(),
        output: local_target,
        capture,
    })
}

pub async fn handle_cluster_list(
    ctx: &AppContext,
    cmd: ListClustersCommand,
) -> AppResult<CommandResult> {
    let clusters = ctx.orbitd.list_clusters("", cmd.check_reachability).await?;
    Ok(CommandResult::ClusterList {
        clusters,
        check_reachability: cmd.check_reachability,
    })
}

pub async fn handle_cluster_get(
    ctx: &AppContext,
    cmd: ClusterGetCommand,
) -> AppResult<CommandResult> {
    let clusters = ctx.orbitd.list_clusters("", true).await?;
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
    let clusters = ctx.orbitd.list_clusters("", true).await?;
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
                && cluster_host_matches(cluster, host)
                && cluster.port == resolved.port as i32
        }) {
            return Err(AppError::conflict(format!(
                "another cluster with name '{}' is already using {}:{}:{}; use 'cluster set' to update it.",
                existing.name, resolved.username, host, resolved.port
            )));
        }
    }

    let mut needs_base_path_prompt = resolved.default_base_path.is_none();
    if let Some(ref value) = resolved.default_base_path {
        if let Err(err) = local_validate_default_base_path(value) {
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
            match local_validate_default_base_path(&base_path) {
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

fn cluster_host_matches(cluster: &ListClustersUnitResponse, host: &str) -> bool {
    match cluster.host.as_ref() {
        Some(proto::list_clusters_unit_response::Host::Hostname(value)) => value == host,
        Some(proto::list_clusters_unit_response::Host::Ipaddr(value)) => value == host,
        None => false,
    }
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

    let clusters = ctx.orbitd.list_clusters("", true).await?;
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

pub async fn handle_project_init(
    ctx: &AppContext,
    cmd: ProjectInitCommand,
) -> AppResult<CommandResult> {
    let init_path = resolve_project_init_path(ctx, &cmd.path)?;
    let parent = init_path.parent().ok_or_else(|| {
        AppError::invalid_argument(format!(
            "cannot initialize project at '{}'",
            init_path.display()
        ))
    })?;
    if !ctx.fs.is_dir(parent)? {
        return Err(AppError::invalid_argument(format!(
            "parent directory does not exist: {}",
            parent.display()
        )));
    }
    if !ctx.fs.is_dir(&init_path)? {
        std::fs::create_dir_all(&init_path).map_err(|err| {
            AppError::local_error(format!("failed to create project directory: {err}"))
        })?;
    }

    let orbitfile_path = init_path.join("Orbitfile");
    let orbitfile_exists = ctx.fs.is_file(&orbitfile_path)?;
    let resolved_name = resolve_project_init_name(ctx, cmd.name, &init_path).await?;
    validate_project_name(&resolved_name)?;

    let contents = if orbitfile_exists {
        let bytes = ctx.fs.read_file(&orbitfile_path)?;
        let existing = String::from_utf8(bytes).map_err(|err| {
            AppError::invalid_argument(format!("invalid Orbitfile encoding: {err}"))
        })?;
        upsert_orbitfile_project_name(Some(existing.as_str()), &resolved_name)?
    } else {
        build_default_orbitfile_contents(&resolved_name)?
    };
    std::fs::write(&orbitfile_path, contents)
        .map_err(|err| AppError::local_error(format!("failed to write Orbitfile: {err}")))?;

    let git_initialized = ensure_git_repository(&init_path)?;
    let mut actions = Vec::new();
    if orbitfile_exists {
        actions.push(ProjectInitAction {
            status: InitActionStatus::Success,
            message: "Orbitfile updated".to_string(),
        });
    } else {
        actions.push(ProjectInitAction {
            status: InitActionStatus::Success,
            message: "Orbitfile created".to_string(),
        });
    }
    if git_initialized {
        actions.push(ProjectInitAction {
            status: InitActionStatus::Success,
            message: "git repository initialized".to_string(),
        });
    }
    let canonical = ctx.fs.canonicalize(&init_path)?;

    Ok(CommandResult::ProjectInit {
        name: resolved_name,
        path: canonical.clone(),
        orbitfile: canonical.join("Orbitfile"),
        git_initialized,
        actions,
    })
}

pub async fn handle_project_build(
    ctx: &AppContext,
    cmd: ProjectBuildCommand,
) -> AppResult<CommandResult> {
    let build_path = resolve_project_init_path(ctx, &cmd.path)?;
    let canonical = ctx.fs.canonicalize(&build_path)?;
    let _project_config = load_project_from_root(ctx.fs.as_ref(), &canonical)?;
    let project = ctx
        .orbitd
        .build_project(canonical.display().to_string(), cmd.package_git)
        .await?;

    Ok(CommandResult::ProjectBuild { project })
}

pub async fn handle_project_submit(
    ctx: &AppContext,
    cmd: ProjectSubmitCommand,
) -> AppResult<CommandResult> {
    let (project_name, project_tag) = parse_project_ref(&cmd.project)?;
    let clusters = ctx.orbitd.list_clusters("", true).await?;
    let cluster = clusters
        .iter()
        .find(|cluster| cluster.name == cmd.cluster)
        .cloned()
        .ok_or_else(|| {
            AppError::cluster_not_found(format!(
                "cluster '{}' not found; use 'cluster add' to create it",
                cmd.cluster
            ))
        })?;
    validate_cluster_live(ctx, &cluster).await?;

    let project = ctx.orbitd.get_project(&cmd.project).await?;
    let project_root = PathBuf::from(&project.path);
    let sbatch_selector =
        SbatchSelector::new(ctx.fs.as_ref(), ctx.interaction.as_ref(), ctx.ui_mode);

    let sbatchscript = if let Some(explicit) = cmd.sbatchscript.as_deref() {
        explicit.to_string()
    } else if let Some(configured) = project.submit_sbatch_script.as_deref() {
        configured.to_string()
    } else {
        sbatch_selector
            .select_from_candidates(&project.sbatch_scripts)
            .await?
    };
    let rules = ProjectRuleSet {
        include: project.sync_include.clone(),
        exclude: project.sync_exclude.clone(),
    };
    let filters = merge_submit_filters(cmd.filters.clone(), &rules);
    let template_values_json = if let Some(template_json) = project.template_config_json.as_deref()
    {
        let template = template_config_from_json(template_json)?;
        let values = resolve_template_values(
            &template,
            cmd.template_preset.as_deref(),
            &cmd.template_fields,
            cmd.fill_defaults,
            &project_root,
            ctx.fs.as_ref(),
            ctx.interaction.as_ref(),
            ctx.output.as_ref(),
            ctx.ui_mode,
            false,
        )
        .await?;
        Some(serde_json::to_string(&values).map_err(|err| {
            AppError::internal_error(format!("failed to serialize templates: {err}"))
        })?)
    } else {
        if cmd.template_preset.is_some() || !cmd.template_fields.is_empty() {
            return Err(AppError::invalid_argument(
                "template values provided but Orbitfile has no [template] section",
            ));
        }
        None
    };

    ctx.output
        .success(&format!("Selected sbatch script: {sbatchscript}"))
        .await?;
    ctx.output.success("Project source: tarball").await?;

    let mut stream_output = ctx.output.stream_output(StreamKind::Submit);
    let capture = ctx
        .orbitd
        .submit_project(
            project_name,
            project_tag,
            cmd.cluster.clone(),
            cmd.remote_path,
            cmd.new_directory,
            cmd.force,
            sbatchscript.clone(),
            filters,
            project.default_retrieve_path.clone(),
            template_values_json,
            &mut *stream_output,
            ctx.interaction.as_ref(),
        )
        .await?;

    if capture.exit_code.unwrap_or(0) != 0 {
        return Err(submit_error(&capture));
    }

    Ok(CommandResult::JobSubmit {
        cluster: cmd.cluster,
        local_path: project.path.clone(),
        sbatchscript,
        capture,
    })
}

pub async fn handle_project_list(
    ctx: &AppContext,
    _cmd: ProjectListCommand,
) -> AppResult<CommandResult> {
    let projects = ctx.orbitd.list_projects().await?;
    let summarized = summarize_projects(projects);
    Ok(CommandResult::ProjectList {
        projects: summarized,
    })
}

pub async fn handle_project_check(
    ctx: &AppContext,
    cmd: ProjectCheckCommand,
) -> AppResult<CommandResult> {
    let projects = match cmd.name {
        Some(name) => vec![ctx.orbitd.get_project(&name).await?],
        None => ctx.orbitd.list_projects().await?,
    };

    let mut statuses = Vec::with_capacity(projects.len());
    for project in projects {
        if ctx.ui_mode.is_interactive() {
            ctx.output
                .info(&format!("checking {}...", project.name))
                .await?;
        }
        let status = check_registered_project(
            ctx.fs.as_ref(),
            &project.name,
            &PathBuf::from(&project.path),
        );
        if ctx.ui_mode.is_interactive() {
            if status.ok {
                ctx.output
                    .success(&format!("{} healthy", status.name))
                    .await?;
            } else if let Some(reason) = status.reason.as_deref() {
                ctx.output
                    .warn(&format!("✗ {} failed check: {}", status.name, reason))
                    .await?;
            } else {
                ctx.output
                    .warn(&format!("✗ {} failed check", status.name))
                    .await?;
            }
        }
        statuses.push(status);
    }

    let checked = statuses.len();
    let failed = statuses.iter().filter(|status| !status.ok).count();
    let passed = checked.saturating_sub(failed);
    if failed > 0 {
        if ctx.ui_mode.is_interactive() {
            ctx.output
                .warn(&format!(
                    "{checked} CHECKED, {failed} FAILED, {passed} PASSED"
                ))
                .await?;
        }
        let details = statuses
            .iter()
            .map(|status| {
                if status.ok {
                    json!({
                        "name": status.name,
                        "ok": true
                    })
                } else {
                    json!({
                        "name": status.name,
                        "ok": false,
                        "reason": status.reason.clone().unwrap_or_else(|| "unknown failure".to_string())
                    })
                }
            })
            .collect::<Vec<_>>();
        return Err(
            AppError::project_check_failed("One or more projects failed checks.")
                .with_details(json!(details)),
        );
    }

    Ok(CommandResult::ProjectCheck { checked })
}

pub async fn handle_project_delete(
    ctx: &AppContext,
    cmd: ProjectDeleteCommand,
) -> AppResult<CommandResult> {
    if !cmd.yes {
        let scope = if cmd.name.contains(':') {
            "from the local registry."
        } else {
            "and all of its tags from the local registry."
        };
        ctx.output
            .info(&format!(
                "WARNING:\nThis will delete project '{}' {}",
                cmd.name, scope
            ))
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

    let deleted = ctx.orbitd.delete_project(&cmd.name).await?;
    if !deleted {
        return Err(AppError::invalid_argument(format!(
            "project name '{}' is not known",
            cmd.name
        )));
    }

    Ok(CommandResult::ProjectDelete { name: cmd.name })
}

fn bytes_to_string(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).to_string()
}

fn resolve_project_init_path(ctx: &AppContext, path: &std::path::Path) -> AppResult<PathBuf> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        Ok(ctx.fs.current_dir()?.join(path))
    }
}

async fn resolve_project_init_name(
    ctx: &AppContext,
    requested: Option<String>,
    init_path: &std::path::Path,
) -> AppResult<String> {
    if let Some(name) = requested {
        let trimmed = name.trim().to_string();
        validate_project_name(&trimmed)?;
        if ctx.ui_mode.is_interactive() {
            ctx.output
                .success(&format!("Project name: {trimmed}"))
                .await?;
        }
        return Ok(trimmed);
    }

    if !ctx.ui_mode.is_interactive() {
        return Err(AppError::invalid_argument(
            "--name is required in non-interactive mode",
        ));
    }

    let default_name = init_path
        .file_name()
        .and_then(|name| name.to_str())
        .map(sanitize_project_name)
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "project".to_string());
    let prompt_value = ctx
        .interaction
        .prompt_line_with_default(
            "Project name: ",
            "Project identifier used by orbit project commands.",
            &default_name,
        )
        .await?;
    let name = prompt_value.trim().to_string();
    validate_project_name(&name)?;
    if ctx.ui_mode.is_interactive() {
        ctx.output.success(&format!("Project name: {name}")).await?;
    }
    Ok(name)
}

fn parse_project_ref(value: &str) -> AppResult<(String, String)> {
    let trimmed = value.trim();
    let Some((name, tag)) = trimmed.split_once(':') else {
        return Err(AppError::invalid_argument(
            "project submit requires <project name:tag>",
        ));
    };
    let name = name.trim();
    let tag = tag.trim();
    if name.is_empty() || tag.is_empty() {
        return Err(AppError::invalid_argument(
            "project submit requires <project name:tag>",
        ));
    }
    validate_project_name(name)?;
    if tag != "latest" && !is_version_tag(tag) {
        return Err(AppError::invalid_argument(format!(
            "invalid project tag '{tag}'; expected latest or yyyymmdd.NNN"
        )));
    }
    Ok((name.to_string(), tag.to_string()))
}

fn summarize_projects(projects: Vec<proto::ProjectRecord>) -> Vec<ProjectListItem> {
    use std::collections::BTreeMap;

    struct SummaryBuilder {
        name: String,
        tags: Vec<String>,
        latest_tag: Option<String>,
        latest_updated: Option<String>,
        latest_path: Option<String>,
        fallback_updated: String,
        fallback_path: String,
    }

    let mut grouped: BTreeMap<String, SummaryBuilder> = BTreeMap::new();

    for project in projects {
        let entry = grouped
            .entry(project.name.clone())
            .or_insert_with(|| SummaryBuilder {
                name: project.name.clone(),
                tags: Vec::new(),
                latest_tag: None,
                latest_updated: None,
                latest_path: None,
                fallback_updated: project.updated_at.clone(),
                fallback_path: project.path.clone(),
            });

        if project.updated_at > entry.fallback_updated {
            entry.fallback_updated = project.updated_at.clone();
            entry.fallback_path = project.path.clone();
        }

        if let Some(tag) = project.version_tag.clone() {
            entry.tags.push(tag.clone());
            let is_newest = entry
                .latest_updated
                .as_deref()
                .map_or(true, |current| project.updated_at.as_str() > current);
            if is_newest {
                entry.latest_tag = Some(tag);
                entry.latest_updated = Some(project.updated_at.clone());
                entry.latest_path = Some(project.path.clone());
            }
        }
    }

    let mut output = Vec::with_capacity(grouped.len());
    for (_, mut entry) in grouped {
        entry.tags.sort();
        entry.tags.dedup();
        let (path, updated_at) = match entry.latest_updated {
            Some(updated) => (entry.latest_path.unwrap_or(entry.fallback_path), updated),
            None => (entry.fallback_path, entry.fallback_updated),
        };
        output.push(ProjectListItem {
            name: entry.name,
            path,
            latest_tag: entry.latest_tag,
            tags: entry.tags,
            updated_at,
        });
    }

    output
}

fn is_version_tag(tag: &str) -> bool {
    let mut parts = tag.splitn(2, '.');
    let Some(date) = parts.next() else {
        return false;
    };
    let Some(suffix) = parts.next() else {
        return false;
    };
    if date.len() != 8 || !date.chars().all(|ch| ch.is_ascii_digit()) {
        return false;
    }
    if suffix.len() != 3 || !suffix.chars().all(|ch| ch.is_ascii_digit()) {
        return false;
    }
    true
}

fn ensure_git_repository(path: &std::path::Path) -> AppResult<bool> {
    if path.join(".git").exists() {
        return Ok(false);
    }
    let status = std::process::Command::new("git")
        .arg("init")
        .arg("--quiet")
        .current_dir(path)
        .status()
        .map_err(|err| AppError::local_error(format!("failed to run git init: {err}")))?;
    if !status.success() {
        return Err(AppError::local_error(format!(
            "git init failed in {}",
            path.display()
        )));
    }
    Ok(true)
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
        if let Some(normalized) = normalize_submit_conflict_message(&message) {
            return AppError::with_exit_code(ErrorType::Conflict, normalized, exit_code);
        }
        return AppError::with_exit_code(ErrorType::RemoteError, message, exit_code);
    }
    if let Some(code) = capture.error_code.as_deref() {
        let kind = error_type_for_remote_code(code, ErrorContext::Job);
        let message = format_server_error(code);
        return AppError::with_exit_code(kind, message, exit_code);
    }
    AppError::with_exit_code(ErrorType::RemoteError, "submission failed", exit_code)
}

fn normalize_submit_conflict_message(message: &str) -> Option<String> {
    let trimmed = message.trim();
    if !trimmed.starts_with("job ") {
        return None;
    }
    let infix = " is still running in ";
    let suffix = "; use --force to submit anyway";
    let (job_prefix, rest) = trimmed.split_once(infix)?;
    let (remote_path, _) = rest.split_once(suffix)?;
    let remote_path = remote_path.trim();
    if remote_path.is_empty() {
        return None;
    }
    Some(format!(
        "Error: {job_prefix}{infix}{remote_path}: use --force to submit anyway"
    ))
}

async fn validate_cluster_live(
    ctx: &AppContext,
    cluster: &ListClustersUnitResponse,
) -> AppResult<()> {
    let host = match cluster.host.as_ref() {
        Some(proto::list_clusters_unit_response::Host::Hostname(value)) => value.as_str(),
        Some(proto::list_clusters_unit_response::Host::Ipaddr(value)) => value.as_str(),
        None => {
            return Err(AppError::invalid_argument(format!(
                "cluster '{}' has no configured host",
                cluster.name
            )));
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
