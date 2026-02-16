// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

use proto::{ListClustersUnitResponse, ListJobsUnitResponse};
use std::collections::HashSet;
use std::path::PathBuf;

use crate::app::AppContext;
use crate::app::commands::*;
use crate::app::errors::{
    AppError, AppResult, ErrorContext, ErrorType, error_type_for_remote_code, format_server_error,
};
use crate::app::ports::{StreamKind, StreamOutputPort};
use crate::app::services::{
    AddClusterResolver, BlueprintRuleSet, PathResolver, SbatchSelector, TemplateSpecialContext,
    build_default_orbitfile_contents, discover_blueprint_from_run_root, load_blueprint_from_root,
    merge_run_filters, resolve_orbitfile_sbatch_script, resolve_template_values,
    sanitize_blueprint_name, template_config_from_json, upsert_orbitfile_blueprint_name,
    validate_blueprint_name,
};

pub async fn handle_ping(ctx: &AppContext, _cmd: PingCommand) -> AppResult<CommandResult> {
    ctx.orbitd.ping().await?;
    Ok(CommandResult::Pong {
        message: "pong".to_string(),
    })
}

pub async fn handle_run(ctx: &AppContext, cmd: RunCommand) -> AppResult<CommandResult> {
    let resolver = PathResolver::new(ctx.fs.as_ref());
    if let Ok(path) = resolver.resolve_local(&cmd.target) {
        if ctx.fs.is_dir(&path)? {
            return handle_job_run(
                ctx,
                JobRunCommand {
                    cluster: cmd.cluster,
                    local_path: cmd.target,
                    sbatchscript: cmd.sbatchscript,
                    remote_path: cmd.remote_path,
                    new_directory: cmd.new_directory,
                    filters: cmd.filters,
                    template_preset: cmd.template_preset,
                    template_fields: cmd.template_fields,
                    fill_defaults: cmd.fill_defaults,
                },
            )
            .await;
        }
    }

    handle_blueprint_run(
        ctx,
        BlueprintRunCommand {
            blueprint: cmd.target,
            cluster: cmd.cluster,
            sbatchscript: cmd.sbatchscript,
            remote_path: cmd.remote_path,
            new_directory: cmd.new_directory,
            filters: cmd.filters,
            template_preset: cmd.template_preset,
            template_fields: cmd.template_fields,
            fill_defaults: cmd.fill_defaults,
        },
    )
    .await
}

pub async fn handle_job_list(ctx: &AppContext, cmd: ListJobsCommand) -> AppResult<CommandResult> {
    let jobs = ctx.orbitd.list_jobs(cmd.cluster, cmd.blueprint).await?;
    Ok(CommandResult::JobList { jobs })
}

pub async fn handle_job_get(ctx: &AppContext, cmd: JobGetCommand) -> AppResult<CommandResult> {
    let jobs = ctx.orbitd.list_jobs(cmd.cluster.clone(), None).await?;
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

pub async fn handle_job_run(ctx: &AppContext, cmd: JobRunCommand) -> AppResult<CommandResult> {
    let clusters = ctx.orbitd.list_clusters("", true).await?;
    let cluster = resolve_cluster_by_name_or_default(&clusters, cmd.cluster.as_deref(), "--on")?;

    validate_cluster_live(ctx, &cluster).await?;

    let resolver = PathResolver::new(ctx.fs.as_ref());
    let resolved_local_path = resolver.resolve_local(&cmd.local_path)?;
    let discovered_blueprint =
        discover_blueprint_from_run_root(ctx.fs.as_ref(), &resolved_local_path)?;
    let sbatch_selector =
        SbatchSelector::new(ctx.fs.as_ref(), ctx.interaction.as_ref(), ctx.ui_mode);
    let sbatchscript = if let Some(explicit) = cmd.sbatchscript.as_deref() {
        sbatch_selector
            .select(&resolved_local_path, Some(explicit))
            .await?
    } else if let Some(blueprint) = discovered_blueprint.as_ref() {
        if let Some(configured) = blueprint.submit_sbatch_script.as_deref() {
            resolve_orbitfile_sbatch_script(
                ctx.fs.as_ref(),
                &blueprint.root,
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
    let mut default_retrieve_path = None;
    let mut template_values_json = None;

    if let Some(blueprint) = discovered_blueprint {
        validate_blueprint_name(&blueprint.name)?;
        filters = merge_run_filters(filters, &blueprint.rules);
        default_retrieve_path = blueprint.default_retrieve_path.clone();
        if let Some(template) = blueprint.template.as_ref() {
            let values = resolve_template_values(
                template,
                cmd.template_preset.as_deref(),
                &cmd.template_fields,
                cmd.fill_defaults,
                &blueprint.root,
                ctx.fs.as_ref(),
                ctx.interaction.as_ref(),
                ctx.output.as_ref(),
                ctx.ui_mode,
                TemplateSpecialContext {
                    cluster_name: &cluster.name,
                    accounting_available: cluster.accounting_available,
                    default_scratch_directory: cluster.default_scratch_directory.as_deref(),
                    orbitd: ctx.orbitd.as_ref(),
                },
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

    let mut stream_output = ctx.output.stream_output(StreamKind::Run);
    let capture = ctx
        .orbitd
        .run_job(
            cluster.name.clone(),
            resolved_local_path_display.clone(),
            cmd.remote_path,
            cmd.new_directory,
            sbatchscript.clone(),
            filters,
            None,
            default_retrieve_path,
            template_values_json,
            &mut *stream_output,
            ctx.interaction.as_ref(),
        )
        .await?;

    if capture.exit_code.unwrap_or(0) != 0 {
        return Err(run_error(&capture));
    }

    Ok(CommandResult::JobSubmit {
        cluster: cluster.name,
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
    let cluster =
        resolve_cluster_by_name_or_default(&clusters, cmd.name.as_deref(), "'cluster get <name>'")?;
    Ok(CommandResult::ClusterDetails { cluster })
}

pub async fn handle_cluster_ls(
    ctx: &AppContext,
    cmd: ClusterLsCommand,
) -> AppResult<CommandResult> {
    let clusters = ctx.orbitd.list_clusters("", true).await?;
    let cluster = resolve_cluster_by_name_or_default(
        &clusters,
        cmd.name.as_deref(),
        "'cluster ls <name> [path]'",
    )?;
    let mut stream_output = ctx.output.stream_output(StreamKind::Generic);
    let capture = ctx
        .orbitd
        .ls(
            cluster.name,
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
    let force_default = cmd.is_default;
    let clusters = ctx.orbitd.list_clusters("", true).await?;
    let planned_is_default = if clusters.is_empty() || force_default {
        Some(true)
    } else if ctx.ui_mode.is_interactive() {
        None
    } else {
        Some(false)
    };
    let existing_names = clusters
        .iter()
        .map(|cluster| cluster.name.clone())
        .collect::<HashSet<_>>();
    let resolver = AddClusterResolver::new(
        ctx.interaction.as_ref(),
        ctx.fs.as_ref(),
        ctx.network.as_ref(),
        ctx.ui_mode,
    );
    let resolved = resolver.resolve(cmd, &existing_names).await?;
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

    let mut stream_output = ctx.output.stream_output(StreamKind::Generic);
    let add_cluster = ctx
        .orbitd
        .add_cluster(
            resolved.name.clone(),
            resolved.username.clone(),
            resolved.hostname.clone(),
            resolved.ip.clone(),
            Some(resolved.identity_path.clone()),
            resolved.port,
            resolved.default_base_path.clone(),
            resolved.default_scratch_directory.clone(),
            ctx.ui_mode.is_interactive(),
            planned_is_default,
            &mut *stream_output,
            ctx.interaction.as_ref(),
        )
        .await?;
    if add_cluster.stream.exit_code.unwrap_or(0) != 0 {
        return Err(stream_error(
            &add_cluster.stream,
            ErrorContext::Cluster,
            "command failed",
        ));
    }

    let default_base_path = add_cluster
        .default_base_path
        .clone()
        .or(resolved.default_base_path.clone());
    let default_scratch_directory = add_cluster.default_scratch_directory;
    let is_default = add_cluster
        .is_default
        .or(planned_is_default)
        .unwrap_or(false);

    Ok(CommandResult::ClusterAdd {
        name: resolved.name,
        username: resolved.username,
        hostname: resolved.hostname,
        ip: resolved.ip,
        port: resolved.port,
        identity_path: resolved.identity_path,
        default_base_path,
        default_scratch_directory,
        is_default,
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
    let cluster = resolve_cluster_by_name_or_default(
        &clusters,
        cmd.name.as_deref(),
        "'cluster set <name> ...'",
    )?;

    if cmd.host.is_none() && cluster.host.is_none() {
        return Err(AppError::invalid_argument(format!(
            "cluster '{}' has no address; pass --host to update it",
            cluster.name
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

    let deleted = ctx.orbitd.delete_cluster(&cmd.name, cmd.force).await?;
    if !deleted {
        return Err(AppError::cluster_not_found(format!(
            "cluster name '{}' is not known",
            cmd.name
        )));
    }

    Ok(CommandResult::ClusterDelete { name: cmd.name })
}

pub async fn handle_blueprint_init(
    ctx: &AppContext,
    cmd: BlueprintInitCommand,
) -> AppResult<CommandResult> {
    let init_path = resolve_blueprint_init_path(ctx, &cmd.path)?;
    let parent = init_path.parent().ok_or_else(|| {
        AppError::invalid_argument(format!(
            "cannot initialize blueprint at '{}'",
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
            AppError::local_error(format!("failed to create blueprint directory: {err}"))
        })?;
    }

    let orbitfile_path = init_path.join("Orbitfile");
    let orbitfile_exists = ctx.fs.is_file(&orbitfile_path)?;
    let resolved_name = resolve_blueprint_init_name(ctx, cmd.name, &init_path).await?;
    validate_blueprint_name(&resolved_name)?;

    let contents = if orbitfile_exists {
        let bytes = ctx.fs.read_file(&orbitfile_path)?;
        let existing = String::from_utf8(bytes).map_err(|err| {
            AppError::invalid_argument(format!("invalid Orbitfile encoding: {err}"))
        })?;
        upsert_orbitfile_blueprint_name(Some(existing.as_str()), &resolved_name)?
    } else {
        build_default_orbitfile_contents(&resolved_name)?
    };
    std::fs::write(&orbitfile_path, contents)
        .map_err(|err| AppError::local_error(format!("failed to write Orbitfile: {err}")))?;

    let git_initialized = ensure_git_repository(&init_path)?;
    let mut actions = Vec::new();
    if orbitfile_exists {
        actions.push(BlueprintInitAction {
            status: InitActionStatus::Success,
            message: "Orbitfile updated".to_string(),
        });
    } else {
        actions.push(BlueprintInitAction {
            status: InitActionStatus::Success,
            message: "Orbitfile created".to_string(),
        });
    }
    if git_initialized {
        actions.push(BlueprintInitAction {
            status: InitActionStatus::Success,
            message: "git repository initialized".to_string(),
        });
    }
    let canonical = ctx.fs.canonicalize(&init_path)?;

    Ok(CommandResult::BlueprintInit {
        name: resolved_name,
        path: canonical.clone(),
        orbitfile: canonical.join("Orbitfile"),
        git_initialized,
        actions,
    })
}

pub async fn handle_blueprint_build(
    ctx: &AppContext,
    cmd: BlueprintBuildCommand,
) -> AppResult<CommandResult> {
    let build_path = resolve_blueprint_init_path(ctx, &cmd.path)?;
    let canonical = ctx.fs.canonicalize(&build_path)?;
    let _blueprint_config = load_blueprint_from_root(ctx.fs.as_ref(), &canonical)?;
    let blueprint = ctx
        .orbitd
        .build_blueprint(canonical.display().to_string(), cmd.package_git)
        .await?;

    Ok(CommandResult::BlueprintBuild { blueprint })
}

pub async fn handle_blueprint_run(
    ctx: &AppContext,
    cmd: BlueprintRunCommand,
) -> AppResult<CommandResult> {
    let (blueprint_name, blueprint_tag) = parse_blueprint_ref(&cmd.blueprint)?;
    let clusters = ctx.orbitd.list_clusters("", true).await?;
    let cluster = resolve_cluster_by_name_or_default(&clusters, cmd.cluster.as_deref(), "--on")?;
    validate_cluster_live(ctx, &cluster).await?;

    let blueprint = ctx.orbitd.get_blueprint(&cmd.blueprint).await?;
    let blueprint_root = PathBuf::from(&blueprint.path);
    let sbatch_selector =
        SbatchSelector::new(ctx.fs.as_ref(), ctx.interaction.as_ref(), ctx.ui_mode);

    let sbatchscript = if let Some(explicit) = cmd.sbatchscript.as_deref() {
        explicit.to_string()
    } else if let Some(configured) = blueprint.submit_sbatch_script.as_deref() {
        configured.to_string()
    } else {
        sbatch_selector
            .select_from_candidates(&blueprint.sbatch_scripts)
            .await?
    };
    let rules = BlueprintRuleSet {
        include: blueprint.sync_include.clone(),
        exclude: blueprint.sync_exclude.clone(),
    };
    let filters = merge_run_filters(cmd.filters.clone(), &rules);
    let template_values_json =
        if let Some(template_json) = blueprint.template_config_json.as_deref() {
            let template = template_config_from_json(template_json)?;
            let values = resolve_template_values(
                &template,
                cmd.template_preset.as_deref(),
                &cmd.template_fields,
                cmd.fill_defaults,
                &blueprint_root,
                ctx.fs.as_ref(),
                ctx.interaction.as_ref(),
                ctx.output.as_ref(),
                ctx.ui_mode,
                TemplateSpecialContext {
                    cluster_name: &cluster.name,
                    accounting_available: cluster.accounting_available,
                    default_scratch_directory: cluster.default_scratch_directory.as_deref(),
                    orbitd: ctx.orbitd.as_ref(),
                },
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
    ctx.output.success("Blueprint source: tarball").await?;

    let mut stream_output = ctx.output.stream_output(StreamKind::Run);
    let capture = ctx
        .orbitd
        .run_blueprint(
            blueprint_name,
            blueprint_tag,
            cluster.name.clone(),
            cmd.remote_path,
            cmd.new_directory,
            sbatchscript.clone(),
            filters,
            blueprint.default_retrieve_path.clone(),
            template_values_json,
            &mut *stream_output,
            ctx.interaction.as_ref(),
        )
        .await?;

    if capture.exit_code.unwrap_or(0) != 0 {
        return Err(run_error(&capture));
    }

    Ok(CommandResult::JobSubmit {
        cluster: cluster.name,
        local_path: blueprint.path.clone(),
        sbatchscript,
        capture,
    })
}

pub async fn handle_blueprint_list(
    ctx: &AppContext,
    _cmd: BlueprintListCommand,
) -> AppResult<CommandResult> {
    let blueprints = ctx.orbitd.list_blueprints().await?;
    let summarized = summarize_blueprints(blueprints);
    Ok(CommandResult::BlueprintList {
        blueprints: summarized,
    })
}

pub async fn handle_blueprint_delete(
    ctx: &AppContext,
    cmd: BlueprintDeleteCommand,
) -> AppResult<CommandResult> {
    if !cmd.yes {
        let scope = if cmd.name.contains(':') {
            "from the local registry."
        } else {
            "and all of its tags from the local registry."
        };
        ctx.output
            .info(&format!(
                "WARNING:\nThis will delete blueprint '{}' {}",
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

    let deleted = ctx.orbitd.delete_blueprint(&cmd.name).await?;
    if !deleted {
        return Err(AppError::invalid_argument(format!(
            "blueprint name '{}' is not known",
            cmd.name
        )));
    }

    Ok(CommandResult::BlueprintDelete { name: cmd.name })
}

fn bytes_to_string(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).to_string()
}

fn resolve_cluster_by_name_or_default(
    clusters: &[ListClustersUnitResponse],
    requested_name: Option<&str>,
    explicit_hint: &str,
) -> AppResult<ListClustersUnitResponse> {
    if let Some(name) = requested_name {
        return clusters
            .iter()
            .find(|cluster| cluster.name == name)
            .cloned()
            .ok_or_else(|| {
                AppError::cluster_not_found(format!(
                    "cluster '{}' not found; use 'cluster add' to create it",
                    name
                ))
            });
    }

    if clusters.is_empty() {
        return Err(AppError::cluster_not_found(
            "no clusters configured; use 'cluster add' to create one",
        ));
    }

    clusters
        .iter()
        .find(|cluster| cluster.is_default)
        .cloned()
        .ok_or_else(|| {
            AppError::cluster_not_found(format!(
                "no default cluster configured; specify a cluster with {}",
                explicit_hint
            ))
        })
}

fn resolve_blueprint_init_path(ctx: &AppContext, path: &std::path::Path) -> AppResult<PathBuf> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        Ok(ctx.fs.current_dir()?.join(path))
    }
}

async fn resolve_blueprint_init_name(
    ctx: &AppContext,
    requested: Option<String>,
    init_path: &std::path::Path,
) -> AppResult<String> {
    if let Some(name) = requested {
        let trimmed = name.trim().to_string();
        validate_blueprint_name(&trimmed)?;
        if ctx.ui_mode.is_interactive() {
            ctx.output
                .success(&format!("Blueprint name: {trimmed}"))
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
        .map(sanitize_blueprint_name)
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "blueprint".to_string());
    let mut prompt = ctx
        .interaction
        .prompt_line_with_default_confirmable(
            "Blueprint name: ",
            "Blueprint identifier used by orbit blueprint commands.",
            &default_name,
        )
        .await?;
    let name = prompt.input.trim().to_string();
    validate_blueprint_name(&name)?;
    prompt.finish_success(&format!("Blueprint name: {name}"))?;
    Ok(name)
}

fn parse_blueprint_ref(value: &str) -> AppResult<(String, String)> {
    let trimmed = value.trim();
    let Some((name, tag)) = trimmed.split_once(':') else {
        return Err(AppError::invalid_argument(
            "blueprint run requires <blueprint name:tag>",
        ));
    };
    let name = name.trim();
    let tag = tag.trim();
    if name.is_empty() || tag.is_empty() {
        return Err(AppError::invalid_argument(
            "blueprint run requires <blueprint name:tag>",
        ));
    }
    validate_blueprint_name(name)?;
    if tag != "latest" && !is_version_tag(tag) {
        return Err(AppError::invalid_argument(format!(
            "invalid blueprint tag '{tag}'; expected latest or yyyymmdd.NNN"
        )));
    }
    Ok((name.to_string(), tag.to_string()))
}

fn summarize_blueprints(blueprints: Vec<proto::BlueprintRecord>) -> Vec<BlueprintListItem> {
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

    for blueprint in blueprints {
        let entry = grouped
            .entry(blueprint.name.clone())
            .or_insert_with(|| SummaryBuilder {
                name: blueprint.name.clone(),
                tags: Vec::new(),
                latest_tag: None,
                latest_updated: None,
                latest_path: None,
                fallback_updated: blueprint.updated_at.clone(),
                fallback_path: blueprint.path.clone(),
            });

        if blueprint.updated_at > entry.fallback_updated {
            entry.fallback_updated = blueprint.updated_at.clone();
            entry.fallback_path = blueprint.path.clone();
        }

        if let Some(tag) = blueprint.version_tag.clone() {
            entry.tags.push(tag.clone());
            let is_newest = entry
                .latest_updated
                .as_deref()
                .map_or(true, |current| blueprint.updated_at.as_str() > current);
            if is_newest {
                entry.latest_tag = Some(tag);
                entry.latest_updated = Some(blueprint.updated_at.clone());
                entry.latest_path = Some(blueprint.path.clone());
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
        output.push(BlueprintListItem {
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

fn run_error(capture: &RunCapture) -> AppError {
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
        if let Some(normalized) = normalize_run_conflict_message(&message) {
            return AppError::with_exit_code(ErrorType::Conflict, normalized, exit_code);
        }
        return AppError::with_exit_code(ErrorType::RemoteError, message, exit_code);
    }
    if let Some(code) = capture.error_code.as_deref() {
        let kind = error_type_for_remote_code(code, ErrorContext::Job);
        let message = format_server_error(code);
        return AppError::with_exit_code(kind, message, exit_code);
    }
    AppError::with_exit_code(ErrorType::RemoteError, "run failed", exit_code)
}

fn normalize_run_conflict_message(message: &str) -> Option<String> {
    let trimmed = message.trim();
    if !trimmed.starts_with("job ") {
        return None;
    }
    let infix = " is still running in ";
    let (job_prefix, rest) = trimmed.split_once(infix)?;
    let suffix =
        "; cancel it first or run in a new directory with --new-directory or --remote-path";
    let legacy_suffix = "; use --force to run anyway";
    let (remote_path, _) = rest
        .split_once(suffix)
        .or_else(|| rest.split_once(legacy_suffix))?;
    let remote_path = remote_path.trim();
    if remote_path.is_empty() {
        return None;
    }
    Some(format!(
        "Error: {job_prefix}{infix}{remote_path}: cancel it first or run in a new directory with --new-directory or --remote-path"
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
    submit: RunCapture,
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

    async fn on_run_status(&mut self, _status: &proto::RunStatus) -> AppResult<()> {
        Ok(())
    }

    async fn on_run_result(&mut self, _result: &proto::RunResult) -> AppResult<()> {
        Ok(())
    }

    fn take_stream_capture(&mut self) -> StreamCapture {
        std::mem::take(&mut self.stream)
    }

    fn take_run_capture(&mut self) -> RunCapture {
        std::mem::take(&mut self.submit)
    }
}
