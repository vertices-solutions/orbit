// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

use anyhow::bail;
use clap::{CommandFactory, FromArgMatches};
use orbit::args::{Cli, ClusterCmd, Cmd, JobCmd};
use orbit::client::{
    fetch_list_clusters, fetch_list_jobs, send_add_cluster, send_add_cluster_capture,
    send_delete_cluster, send_job_cancel, send_job_cancel_capture, send_job_cleanup,
    send_job_cleanup_capture, send_job_logs, send_job_logs_capture, send_job_ls,
    send_job_ls_capture, send_job_retrieve, send_job_retrieve_capture, send_ls, send_ls_capture,
    send_ping, send_resolve_home_dir, send_submit, send_submit_capture, validate_cluster_live,
};
use orbit::config;
use orbit::errors::format_server_error;
use orbit::filters::submit_filters_from_matches;
use orbit::format::{
    cluster_host_string, cluster_to_json, format_cluster_details, format_cluster_details_json,
    format_clusters_json, format_clusters_table, format_job_details, format_job_details_json,
    format_jobs_json, format_jobs_table, format_json, job_to_json,
};
use orbit::interactive::{
    confirm_action, prompt_default_base_path, resolve_add_cluster_args,
    validate_default_base_path_with_feedback,
};
use orbit::interaction;
use orbit::non_interactive::{NonInteractiveError, json_error, json_ok};
use orbit::sbatch::resolve_sbatch_script;
use orbit::stream::{CapturedStream, SubmitCapture, print_with_green_check_stdout};
use proto::ListJobsUnitResponse;
use proto::agent_client::AgentClient;
use proto::SubmitPathFilterRule;
use serde_json::{Value, json};
use std::io::Write;
use std::path::PathBuf;
use tonic::transport::Channel;

const HELP_TEMPLATE: &str = r#"██████╗ ██████╗ ██████╗ ██╗ ████████╗
██╔══██╗██╔══██╗██╔══██╗██║ ╚══██╔══╝
██║  ██║██████╔╝██████╔╝██║    ██║
██║  ██║██╔══██╗██╔══██╗██║    ██║
╚█████╔╝██║  ██║██████╔╝██║    ██║
 ╚════╝ ╚═╝  ╚═╝╚═════╝ ╚═╝    ╚═╝

{before-help}{about-with-newline}{usage-heading} {usage}

{all-args}{after-help}
"#;

fn apply_help_template_recursively(cmd: &mut clap::Command) {
    let mut owned = std::mem::take(cmd);
    owned = owned.help_template(HELP_TEMPLATE);
    for sub in owned.get_subcommands_mut() {
        apply_help_template_recursively(sub);
    }
    *cmd = owned;
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut cmd = Cli::command();
    apply_help_template_recursively(&mut cmd);
    let matches = cmd.get_matches();
    let cli = Cli::from_arg_matches(&matches).unwrap_or_else(|err| err.exit());
    let submit_filters = submit_filters_from_matches(&matches);
    let daemon_endpoint = config::daemon_endpoint(cli.config.clone())?;
    if cli.non_interactive {
        interaction::set_non_interactive(true);
        return run_non_interactive(cli, submit_filters, daemon_endpoint).await;
    }
    match cli.cmd {
        Cmd::Ping => {
            let mut client = connect_orbitd_interactive(&daemon_endpoint).await?;
            match send_ping(&mut client).await {
                Ok(()) => println!("pong"),
                Err(e) => bail!(e),
            }
        }
        Cmd::Job(job_args) => {
            let mut client = connect_orbitd_interactive(&daemon_endpoint).await?;
            match job_args.cmd {
                JobCmd::Submit(args) => {
                    let local_path_buf = PathBuf::from(&args.local_path);
                    let response = fetch_list_clusters(&mut client, "").await?;
                    let Some(cluster) = response
                        .clusters
                        .iter()
                        .find(|cluster| cluster.name == args.name)
                    else {
                        bail!(
                            "cluster '{}' not found; use 'cluster add' to create it",
                            args.name
                        );
                    };
                    validate_cluster_live(&mut client, cluster, false).await?;
                    let resolved_local_path = local_path_buf.canonicalize().map_err(|e| {
                        anyhow::anyhow!(
                            "failed to resolve local path '{}': {e}",
                            local_path_buf.display()
                        )
                    })?;
                    let sbatchscript = resolve_sbatch_script(
                        &local_path_buf,
                        args.sbatchscript.as_deref(),
                        args.headless,
                    )?;
                    let resolved_local_path_display = resolved_local_path.display().to_string();
                    print_with_green_check_stdout(&format!(
                        "Selected sbatch script: {sbatchscript}"
                    ))?;
                    print_with_green_check_stdout(&format!(
                        "Local path: {resolved_local_path_display}"
                    ))?;
                    let _ = std::io::stdout().flush();
                    send_submit(
                        &mut client,
                        &args.name,
                        &resolved_local_path_display,
                        &args.remote_path,
                        args.new_directory,
                        args.force,
                        &sbatchscript,
                        &submit_filters,
                    )
                    .await?
                }
                JobCmd::List(args) => {
                    let response = fetch_list_jobs(&mut client, args.cluster).await?;
                    if args.json {
                        let output = format_jobs_json(&response.jobs)?;
                        println!("{output}");
                    } else {
                        print!("{}", format_jobs_table(&response.jobs));
                    }
                }
                JobCmd::Get(args) => {
                    let response = fetch_list_jobs(&mut client, args.cluster.clone()).await?;
                    let matches: Vec<&ListJobsUnitResponse> = response
                        .jobs
                        .iter()
                        .filter(|job| job.job_id == args.job_id)
                        .collect();
                    match matches.as_slice() {
                        [] => {
                            if let Some(cluster) = args.cluster.as_deref() {
                                bail!("job id {} not found in cluster '{}'", args.job_id, cluster);
                            }
                            bail!("job id {} not found", args.job_id);
                        }
                        [job] => {
                            if args.json {
                                let output = format_job_details_json(job)?;
                                println!("{output}");
                            } else {
                                print!("{}", format_job_details(job));
                            }
                        }
                        _ => {
                            if args.cluster.is_some() {
                                bail!("multiple jobs matched job id {}", args.job_id);
                            }
                            bail!(
                                "job id {} matched multiple clusters; use --cluster",
                                args.job_id
                            );
                        }
                    }
                }
                JobCmd::Logs(args) => {
                    let code = send_job_logs(&mut client, args.job_id, args.err).await?;
                    if code != 0 {
                        std::process::exit(code);
                    }
                }
                JobCmd::Cancel(args) => {
                    if !args.yes {
                        let confirmed = confirm_action(
                            &format!("Cancel job {}? (yes/no): ", args.job_id),
                            "Type yes to confirm, no to cancel.",
                        )?;
                        if !confirmed {
                            println!("Cancel canceled.");
                            return Ok(());
                        }
                    }
                    send_job_cancel(&mut client, args.job_id).await?
                }
                JobCmd::Cleanup(args) => {
                    if !args.yes {
                        println!(
                            "WARNING:\nThis will delete the remote directory for job {}.",
                            args.job_id
                        );
                        if args.force {
                            println!("The job will be canceled before cleanup.");
                        } else {
                            println!("If the job is still running, pass --force to cancel it.");
                        }
                        if args.full {
                            println!("The job record will be deleted from the local database.");
                        }
                        println!("This action cannot be undone.");
                        let confirmed = confirm_action(
                            "Continue with cleanup? (yes/no): ",
                            "Type yes to confirm, no to cancel.",
                        )?;
                        if !confirmed {
                            println!("Cleanup canceled.");
                            return Ok(());
                        }
                    }
                    let exit_code =
                        send_job_cleanup(&mut client, args.job_id, args.force, args.full).await?;
                    if exit_code != 0 {
                        std::process::exit(exit_code);
                    }
                }
                JobCmd::Ls(args) => {
                    send_job_ls(&mut client, args.job_id, &args.path, &args.cluster).await?
                }
                JobCmd::Retrieve(args) => {
                    let code = send_job_retrieve(
                        &mut client,
                        args.job_id,
                        &args.path,
                        &args.output,
                        args.overwrite,
                        args.force,
                        args.headless,
                    )
                    .await?;
                    if code != 0 {
                        std::process::exit(code);
                    }
                }
            }
        }
        Cmd::Cluster(cluster_args) => {
            let mut client = connect_orbitd_interactive(&daemon_endpoint).await?;
            match cluster_args.cmd {
                ClusterCmd::List(args) => {
                    let response = fetch_list_clusters(&mut client, "").await?;
                    if args.json {
                        let output = format_clusters_json(&response.clusters)?;
                        println!("{output}");
                    } else {
                        print!("{}", format_clusters_table(&response.clusters));
                    }
                }
                ClusterCmd::Get(args) => {
                    let response = fetch_list_clusters(&mut client, "").await?;
                    let Some(cluster) = response
                        .clusters
                        .iter()
                        .find(|cluster| cluster.name == args.name)
                    else {
                        bail!("cluster '{}' not found", args.name);
                    };
                    if args.json {
                        let output = format_cluster_details_json(cluster)?;
                        println!("{output}");
                    } else {
                        print!("{}", format_cluster_details(cluster));
                    }
                }
                ClusterCmd::Ls(args) => {
                    send_ls(&mut client, &args.name, &args.path).await?
                }
                ClusterCmd::Add(args) => {
                    println!("Adding new cluster...");
                    let headless = args.headless;
                    let response = fetch_list_clusters(&mut client, "").await?;
                    let existing_names = response
                        .clusters
                        .iter()
                        .map(|cluster| cluster.name.clone())
                        .collect::<std::collections::HashSet<_>>();
                    let mut resolved = resolve_add_cluster_args(args, &existing_names)?;
                    if let Some(host) = resolved.hostname.as_deref().or(resolved.ip.as_deref()) {
                        if let Some(existing) = response.clusters.iter().find(|cluster| {
                            cluster.username == resolved.username
                                && cluster_host_string(cluster) == host
                                && cluster.port == resolved.port as i32
                        }) {
                            bail!(
                                "another cluster with name '{}' is already using {}:{}:{}; use 'cluster set' to update it.",
                                existing.name,
                                resolved.username,
                                host,
                                resolved.port
                            );
                        }
                    }
                    let mut needs_base_path_prompt = resolved.default_base_path.is_none();
                    if let Some(ref value) = resolved.default_base_path {
                        if let Err(err) =
                            validate_default_base_path_with_feedback(value, false, headless)
                        {
                            if headless {
                                return Err(err);
                            }
                            eprintln!("Default base path '{value}' is invalid: {err}");
                            needs_base_path_prompt = true;
                        }
                    }
                    if needs_base_path_prompt {
                        let home_dir = send_resolve_home_dir(
                            &mut client,
                            &resolved.name,
                            &resolved.username,
                            &resolved.hostname,
                            &resolved.ip,
                            Some(resolved.identity_path.as_str()),
                            resolved.port,
                        )
                        .await?;
                        loop {
                            let base_path = prompt_default_base_path(&home_dir)?;
                            match validate_default_base_path_with_feedback(
                                &base_path,
                                true,
                                headless,
                            ) {
                                Ok(()) => {
                                    resolved.default_base_path = Some(base_path);
                                    break;
                                }
                                Err(err) => {
                                    if headless {
                                        return Err(err);
                                    }
                                    eprintln!("Default base path '{base_path}' is invalid: {err}");
                                }
                            }
                        }
                    }
                    send_add_cluster(
                        &mut client,
                        &resolved.name,
                        &resolved.username,
                        &resolved.hostname,
                        &resolved.ip,
                        Some(resolved.identity_path.as_str()),
                        resolved.port,
                        &resolved.default_base_path,
                        true,
                    )
                    .await?;
                    println!("Cluster {} added successfully!", resolved.name);
                }
                ClusterCmd::Set(args) => {
                    let response = fetch_list_clusters(&mut client, "").await?;
                    let Some(cluster) = response
                        .clusters
                        .iter()
                        .find(|cluster| cluster.name == args.name)
                    else {
                        bail!(
                            "cluster '{}' not found; use 'cluster add' to create it",
                            args.name
                        );
                    };
                    let (hostname, ip) = match args.ip.as_ref() {
                        Some(ip) => (None, Some(ip.clone())),
                        None => match &cluster.host {
                            Some(proto::list_clusters_unit_response::Host::Hostname(host)) => {
                                (Some(host.clone()), None)
                            }
                            Some(proto::list_clusters_unit_response::Host::Ipaddr(host)) => {
                                (None, Some(host.clone()))
                            }
                            None => {
                                bail!(
                                    "cluster '{}' has no address; pass --ip to update it",
                                    args.name
                                );
                            }
                        },
                    };
                    let port = match args.port {
                        Some(port) => port,
                        None => match u32::try_from(cluster.port) {
                            Ok(port) => port,
                            Err(_) => bail!(
                                "cluster '{}' has invalid port '{}'",
                                args.name,
                                cluster.port
                            ),
                        },
                    };
                    let identity_path = args
                        .identity_path
                        .clone()
                        .or_else(|| cluster.identity_path.clone());
                    let default_base_path = args
                        .default_base_path
                        .clone()
                        .or_else(|| cluster.default_base_path.clone());
                    send_add_cluster(
                        &mut client,
                        &cluster.name,
                        &cluster.username,
                        &hostname,
                        &ip,
                        identity_path.as_deref(),
                        port,
                        &default_base_path,
                        false,
                    )
                    .await?
                }
                ClusterCmd::Delete(args) => {
                    let response = fetch_list_clusters(&mut client, "").await?;
                    if !response
                        .clusters
                        .iter()
                        .any(|cluster| cluster.name == args.name)
                    {
                        bail!("cluster name '{}' is not known", args.name);
                    }
                    println!(
                        "WARNING:\nThis will delete cluster '{}' and all its job records from the local database.",
                        args.name
                    );
                    println!("Any active SSH sessions for this cluster will be closed.");
                    println!("This action cannot be undone.");
                    if !args.yes {
                        let confirmed = confirm_action(
                            "Continue with delete? (yes/no): ",
                            "Type yes to confirm, no to cancel.",
                        )?;
                        if !confirmed {
                            println!("Delete canceled.");
                            return Ok(());
                        }
                    }
                    let response = send_delete_cluster(&mut client, &args.name).await?;
                    if !response.deleted {
                        bail!("cluster name '{}' is not known", args.name);
                    }
                    println!("Cluster '{}' deleted.", args.name);
                }
            }
        }
    }
    Ok(())
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

fn stream_error_message(captured: &CapturedStream) -> String {
    if let Some(message) = message_from_bytes(&captured.stderr) {
        return message;
    }
    if let Some(code) = captured.error_code.as_deref() {
        return format_server_error(code);
    }
    "command failed".to_string()
}

fn submit_error_message(captured: &SubmitCapture) -> String {
    if let Some(detail) = captured.detail.as_deref() {
        let trimmed = detail.trim();
        if !trimmed.is_empty() {
            return format_server_error(trimmed);
        }
    }
    if let Some(message) = message_from_bytes(&captured.stderr) {
        return message;
    }
    if let Some(code) = captured.error_code.as_deref() {
        return format_server_error(code);
    }
    "submission failed".to_string()
}

fn captured_stream_json(captured: &CapturedStream) -> Value {
    json!({
        "stdout": bytes_to_string(&captured.stdout),
        "stderr": bytes_to_string(&captured.stderr),
        "exit_code": captured.exit_code.unwrap_or(0),
    })
}

fn coerce_non_interactive_error(err: anyhow::Error) -> NonInteractiveError {
    if let Some(non_interactive) = err.downcast_ref::<NonInteractiveError>() {
        return non_interactive.clone();
    }
    NonInteractiveError::other(err.to_string())
}

fn daemon_unavailable_message(daemon_endpoint: &str) -> String {
    format!(
        "Could not contact the orbitd server at {daemon_endpoint}. Is it running?"
    )
}

async fn connect_orbitd_interactive(
    daemon_endpoint: &str,
) -> anyhow::Result<AgentClient<Channel>> {
    AgentClient::connect(daemon_endpoint.to_string())
        .await
        .map_err(|_| anyhow::anyhow!(daemon_unavailable_message(daemon_endpoint)))
}

async fn connect_orbitd_non_interactive(
    daemon_endpoint: &str,
) -> anyhow::Result<AgentClient<Channel>> {
    AgentClient::connect(daemon_endpoint.to_string())
        .await
        .map_err(|_| {
            NonInteractiveError::daemon_unavailable(daemon_unavailable_message(daemon_endpoint))
                .into()
        })
}

async fn run_non_interactive(
    cli: Cli,
    submit_filters: Vec<SubmitPathFilterRule>,
    daemon_endpoint: String,
) -> anyhow::Result<()> {
    let result = run_non_interactive_impl(cli, submit_filters, daemon_endpoint).await;
    match result {
        Ok(value) => {
            let output = format_json(json_ok(value))?;
            println!("{output}");
            Ok(())
        }
        Err(err) => {
            let non_interactive = coerce_non_interactive_error(err);
            let output = format_json(json_error(&non_interactive))?;
            eprintln!("{output}");
            std::process::exit(non_interactive.exit_code);
        }
    }
}

async fn run_non_interactive_impl(
    cli: Cli,
    submit_filters: Vec<SubmitPathFilterRule>,
    daemon_endpoint: String,
) -> anyhow::Result<Value> {
    match cli.cmd {
        Cmd::Ping => {
            let mut client = connect_orbitd_non_interactive(&daemon_endpoint).await?;
            send_ping(&mut client).await?;
            Ok(json!({ "message": "pong" }))
        }
        Cmd::Job(job_args) => {
            let mut client = connect_orbitd_non_interactive(&daemon_endpoint).await?;
            match job_args.cmd {
                JobCmd::Submit(args) => {
                    let local_path_buf = PathBuf::from(&args.local_path);
                    let response = fetch_list_clusters(&mut client, "").await?;
                    let Some(cluster) = response
                        .clusters
                        .iter()
                        .find(|cluster| cluster.name == args.name)
                    else {
                        bail!(
                            "cluster '{}' not found; use 'cluster add' to create it",
                            args.name
                        );
                    };
                    validate_cluster_live(&mut client, cluster, true).await?;
                    let resolved_local_path = local_path_buf.canonicalize().map_err(|e| {
                        anyhow::anyhow!(
                            "failed to resolve local path '{}': {e}",
                            local_path_buf.display()
                        )
                    })?;
                    let sbatchscript = resolve_sbatch_script(
                        &local_path_buf,
                        args.sbatchscript.as_deref(),
                        true,
                    )
                    .map_err(|err| NonInteractiveError::missing_input(err.to_string()))?;
                    let resolved_local_path_display = resolved_local_path.display().to_string();
                    let captured = send_submit_capture(
                        &mut client,
                        &args.name,
                        &resolved_local_path_display,
                        &args.remote_path,
                        args.new_directory,
                        args.force,
                        &sbatchscript,
                        &submit_filters,
                    )
                    .await?;
                    let exit_code = captured.exit_code.unwrap_or(0);
                    if exit_code != 0 {
                        let message = submit_error_message(&captured);
                        return Err(
                            NonInteractiveError::other_with_exit_code(message, exit_code).into(),
                        );
                    }
                    Ok(json!({
                        "job_id": captured.job_id,
                        "remote_path": captured.remote_path,
                        "local_path": resolved_local_path_display,
                        "cluster": args.name,
                        "sbatchscript": sbatchscript,
                        "status": "submitted",
                        "stdout": bytes_to_string(&captured.stdout),
                        "stderr": bytes_to_string(&captured.stderr),
                    }))
                }
                JobCmd::List(args) => {
                    let response = fetch_list_jobs(&mut client, args.cluster).await?;
                    let data: Vec<Value> = response.jobs.iter().map(job_to_json).collect();
                    Ok(Value::Array(data))
                }
                JobCmd::Get(args) => {
                    let response = fetch_list_jobs(&mut client, args.cluster.clone()).await?;
                    let matches: Vec<&ListJobsUnitResponse> = response
                        .jobs
                        .iter()
                        .filter(|job| job.job_id == args.job_id)
                        .collect();
                    match matches.as_slice() {
                        [] => {
                            if let Some(cluster) = args.cluster.as_deref() {
                                bail!("job id {} not found in cluster '{}'", args.job_id, cluster);
                            }
                            bail!("job id {} not found", args.job_id);
                        }
                        [job] => Ok(job_to_json(job)),
                        _ => {
                            if args.cluster.is_some() {
                                bail!("multiple jobs matched job id {}", args.job_id);
                            }
                            bail!(
                                "job id {} matched multiple clusters; use --cluster",
                                args.job_id
                            );
                        }
                    }
                }
                JobCmd::Logs(args) => {
                    let captured =
                        send_job_logs_capture(&mut client, args.job_id, args.err).await?;
                    let exit_code = captured.exit_code.unwrap_or(0);
                    if exit_code != 0 {
                        let message = stream_error_message(&captured);
                        return Err(
                            NonInteractiveError::other_with_exit_code(message, exit_code).into(),
                        );
                    }
                    Ok(captured_stream_json(&captured))
                }
                JobCmd::Cancel(args) => {
                    if !args.yes {
                        return Err(NonInteractiveError::confirmation_required(
                            "confirmation required; pass --yes to proceed in non-interactive mode",
                        )
                        .into());
                    }
                    let captured = send_job_cancel_capture(&mut client, args.job_id).await?;
                    let exit_code = captured.exit_code.unwrap_or(0);
                    if exit_code != 0 {
                        let message = stream_error_message(&captured);
                        return Err(
                            NonInteractiveError::other_with_exit_code(message, exit_code).into(),
                        );
                    }
                    Ok(json!({
                        "job_id": args.job_id,
                        "status": "canceled",
                        "stdout": bytes_to_string(&captured.stdout),
                        "stderr": bytes_to_string(&captured.stderr),
                    }))
                }
                JobCmd::Cleanup(args) => {
                    if !args.yes {
                        return Err(NonInteractiveError::confirmation_required(
                            "confirmation required; pass --yes to proceed in non-interactive mode",
                        )
                        .into());
                    }
                    let captured = send_job_cleanup_capture(
                        &mut client,
                        args.job_id,
                        args.force,
                        args.full,
                    )
                    .await?;
                    let exit_code = captured.exit_code.unwrap_or(0);
                    if exit_code != 0 {
                        let message = stream_error_message(&captured);
                        return Err(
                            NonInteractiveError::other_with_exit_code(message, exit_code).into(),
                        );
                    }
                    Ok(json!({
                        "job_id": args.job_id,
                        "status": "cleaned",
                        "force": args.force,
                        "full": args.full,
                        "stdout": bytes_to_string(&captured.stdout),
                        "stderr": bytes_to_string(&captured.stderr),
                    }))
                }
                JobCmd::Ls(args) => {
                    let captured =
                        send_job_ls_capture(&mut client, args.job_id, &args.path, &args.cluster)
                            .await?;
                    let exit_code = captured.exit_code.unwrap_or(0);
                    if exit_code != 0 {
                        let message = stream_error_message(&captured);
                        return Err(
                            NonInteractiveError::other_with_exit_code(message, exit_code).into(),
                        );
                    }
                    Ok(captured_stream_json(&captured))
                }
                JobCmd::Retrieve(args) => {
                    let captured = send_job_retrieve_capture(
                        &mut client,
                        args.job_id,
                        &args.path,
                        &args.output,
                        args.overwrite,
                        args.force,
                    )
                    .await?;
                    let exit_code = captured.stream.exit_code.unwrap_or(0);
                    if exit_code != 0 {
                        let message = stream_error_message(&captured.stream);
                        return Err(
                            NonInteractiveError::other_with_exit_code(message, exit_code).into(),
                        );
                    }
                    Ok(json!({
                        "job_id": args.job_id,
                        "path": args.path,
                        "output": captured.local_target.display().to_string(),
                        "stdout": bytes_to_string(&captured.stream.stdout),
                        "stderr": bytes_to_string(&captured.stream.stderr),
                        "exit_code": exit_code,
                    }))
                }
            }
        }
        Cmd::Cluster(cluster_args) => {
            let mut client = connect_orbitd_non_interactive(&daemon_endpoint).await?;
            match cluster_args.cmd {
                ClusterCmd::List(args) => {
                    let response = fetch_list_clusters(&mut client, "").await?;
                    let data: Vec<Value> = response.clusters.iter().map(cluster_to_json).collect();
                    Ok(Value::Array(data))
                }
                ClusterCmd::Get(args) => {
                    let response = fetch_list_clusters(&mut client, "").await?;
                    let Some(cluster) = response
                        .clusters
                        .iter()
                        .find(|cluster| cluster.name == args.name)
                    else {
                        bail!("cluster '{}' not found", args.name);
                    };
                    Ok(cluster_to_json(cluster))
                }
                ClusterCmd::Ls(args) => {
                    let captured = send_ls_capture(&mut client, &args.name, &args.path).await?;
                    let exit_code = captured.exit_code.unwrap_or(0);
                    if exit_code != 0 {
                        let message = stream_error_message(&captured);
                        return Err(
                            NonInteractiveError::other_with_exit_code(message, exit_code).into(),
                        );
                    }
                    Ok(captured_stream_json(&captured))
                }
                ClusterCmd::Add(mut args) => {
                    args.headless = true;
                    let response = fetch_list_clusters(&mut client, "").await?;
                    let existing_names = response
                        .clusters
                        .iter()
                        .map(|cluster| cluster.name.clone())
                        .collect::<std::collections::HashSet<_>>();
                    let resolved = resolve_add_cluster_args(args, &existing_names)
                        .map_err(|err| NonInteractiveError::missing_input(err.to_string()))?;
                    if let Some(host) = resolved.hostname.as_deref().or(resolved.ip.as_deref()) {
                        if let Some(existing) = response.clusters.iter().find(|cluster| {
                            cluster.username == resolved.username
                                && cluster_host_string(cluster) == host
                                && cluster.port == resolved.port as i32
                        }) {
                            bail!(
                                "another cluster with name '{}' is already using {}:{}:{}; use 'cluster set' to update it.",
                                existing.name,
                                resolved.username,
                                host,
                                resolved.port
                            );
                        }
                    }
                    let captured = send_add_cluster_capture(
                        &mut client,
                        &resolved.name,
                        &resolved.username,
                        &resolved.hostname,
                        &resolved.ip,
                        Some(resolved.identity_path.as_str()),
                        resolved.port,
                        &resolved.default_base_path,
                    )
                    .await?;
                    let exit_code = captured.exit_code.unwrap_or(0);
                    if exit_code != 0 {
                        let message = stream_error_message(&captured);
                        return Err(
                            NonInteractiveError::other_with_exit_code(message, exit_code).into(),
                        );
                    }
                    Ok(json!({
                        "name": resolved.name,
                        "username": resolved.username,
                        "hostname": resolved.hostname,
                        "ip": resolved.ip,
                        "port": resolved.port,
                        "identity_path": resolved.identity_path,
                        "default_base_path": resolved.default_base_path,
                        "status": "added",
                    }))
                }
                ClusterCmd::Set(args) => {
                    let response = fetch_list_clusters(&mut client, "").await?;
                    let Some(cluster) = response
                        .clusters
                        .iter()
                        .find(|cluster| cluster.name == args.name)
                    else {
                        bail!(
                            "cluster '{}' not found; use 'cluster add' to create it",
                            args.name
                        );
                    };
                    let (hostname, ip) = match args.ip.as_ref() {
                        Some(ip) => (None, Some(ip.clone())),
                        None => match &cluster.host {
                            Some(proto::list_clusters_unit_response::Host::Hostname(host)) => {
                                (Some(host.clone()), None)
                            }
                            Some(proto::list_clusters_unit_response::Host::Ipaddr(host)) => {
                                (None, Some(host.clone()))
                            }
                            None => {
                                bail!(
                                    "cluster '{}' has no address; pass --ip to update it",
                                    args.name
                                );
                            }
                        },
                    };
                    let port = match args.port {
                        Some(port) => port,
                        None => match u32::try_from(cluster.port) {
                            Ok(port) => port,
                            Err(_) => bail!(
                                "cluster '{}' has invalid port '{}'",
                                args.name,
                                cluster.port
                            ),
                        },
                    };
                    let identity_path = args
                        .identity_path
                        .clone()
                        .or_else(|| cluster.identity_path.clone());
                    let default_base_path = args
                        .default_base_path
                        .clone()
                        .or_else(|| cluster.default_base_path.clone());
                    let captured = send_add_cluster_capture(
                        &mut client,
                        &cluster.name,
                        &cluster.username,
                        &hostname,
                        &ip,
                        identity_path.as_deref(),
                        port,
                        &default_base_path,
                    )
                    .await?;
                    let exit_code = captured.exit_code.unwrap_or(0);
                    if exit_code != 0 {
                        let message = stream_error_message(&captured);
                        return Err(
                            NonInteractiveError::other_with_exit_code(message, exit_code).into(),
                        );
                    }
                    Ok(json!({
                        "name": cluster.name.as_str(),
                        "status": "updated",
                    }))
                }
                ClusterCmd::Delete(args) => {
                    if !args.yes {
                        return Err(NonInteractiveError::confirmation_required(
                            "confirmation required; pass --yes to proceed in non-interactive mode",
                        )
                        .into());
                    }
                    let response = send_delete_cluster(&mut client, &args.name).await?;
                    if !response.deleted {
                        bail!("cluster name '{}' is not known", args.name);
                    }
                    Ok(json!({
                        "name": args.name,
                        "status": "deleted",
                    }))
                }
            }
        }
    }
}
