// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

mod args;
mod filters;

pub use args::{Cli, Cmd};

use clap::{ArgMatches, CommandFactory};
use clap_complete::{Shell, generate};

use crate::app::commands::*;
use args::{ClusterCmd, JobCmd, ProjectCmd};
use filters::submit_filters_from_matches;

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
    let mut owned = std::mem::take(cmd); //take to avoid memory allocation
    owned = owned.help_template(HELP_TEMPLATE);
    for sub in owned.get_subcommands_mut() {
        apply_help_template_recursively(sub);
    }
    *cmd = owned;
}

pub fn cli_command() -> clap::Command {
    let mut cmd = Cli::command();
    apply_help_template_recursively(&mut cmd);
    cmd
}

pub fn generate_shell_completions(shell: Shell) {
    let mut cmd = cli_command();
    let bin_name = cmd.get_name().to_owned();
    generate(shell, &mut cmd, bin_name, &mut std::io::stdout());
}

pub fn command_from_cli(cli: Cli, matches: &ArgMatches) -> Command {
    match cli.cmd {
        Cmd::Ping => Command::Ping(PingCommand),
        Cmd::Job(job_args) => Command::Job(match job_args.cmd {
            JobCmd::Submit(args) => {
                let filters = submit_filters_from_matches(matches);
                JobCommand::Submit(SubmitJobCommand {
                    name: args.name,
                    local_path: args.local_path,
                    sbatchscript: args.sbatchscript,
                    remote_path: args.remote_path,
                    new_directory: args.new_directory,
                    force: args.force,
                    filters,
                })
            }
            JobCmd::List(args) => JobCommand::List(ListJobsCommand {
                cluster: args.cluster,
            }),
            JobCmd::Get(args) => JobCommand::Get(JobGetCommand {
                job_id: args.job_id,
                cluster: args.cluster,
            }),
            JobCmd::Logs(args) => JobCommand::Logs(JobLogsCommand {
                job_id: args.job_id,
                err: args.err,
            }),
            JobCmd::Cancel(args) => JobCommand::Cancel(JobCancelCommand {
                job_id: args.job_id,
                yes: args.yes,
            }),
            JobCmd::Cleanup(args) => JobCommand::Cleanup(JobCleanupCommand {
                job_id: args.job_id,
                force: args.force,
                full: args.full,
                yes: args.yes,
            }),
            JobCmd::Ls(args) => JobCommand::Ls(JobLsCommand {
                job_id: args.job_id,
                path: args.path,
                cluster: args.cluster,
            }),
            JobCmd::Retrieve(args) => JobCommand::Retrieve(JobRetrieveCommand {
                job_id: args.job_id,
                path: args.path,
                output: args.output,
                overwrite: args.overwrite,
                force: args.force,
            }),
        }),
        Cmd::Cluster(cluster_args) => Command::Cluster(match cluster_args.cmd {
            ClusterCmd::List(_args) => ClusterCommand::List(ListClustersCommand),
            ClusterCmd::Get(args) => ClusterCommand::Get(ClusterGetCommand { name: args.name }),
            ClusterCmd::Ls(args) => ClusterCommand::Ls(ClusterLsCommand {
                name: args.name,
                path: args.path,
            }),
            ClusterCmd::Add(args) => ClusterCommand::Add(AddClusterCommand {
                destination: args.destination,
                name: args.name,
                identity_path: args.identity_path,
                default_base_path: args.default_base_path,
            }),
            ClusterCmd::Set(args) => ClusterCommand::Set(SetClusterCommand {
                name: args.name,
                host: args.host,
                username: args.username,
                port: args.port,
                identity_path: args.identity_path,
                default_base_path: args.default_base_path,
            }),
            ClusterCmd::Delete(args) => ClusterCommand::Delete(DeleteClusterCommand {
                name: args.name,
                yes: args.yes,
            }),
        }),
        Cmd::Project(project_args) => Command::Project(match project_args.cmd {
            ProjectCmd::Init(args) => ProjectCommand::Init(ProjectInitCommand {
                path: args.path,
                name: args.name,
            }),
            ProjectCmd::Submit(args) => {
                let filters = submit_filters_from_matches(matches);
                ProjectCommand::Submit(ProjectSubmitCommand {
                    project: args.project,
                    cluster: args.cluster,
                    sbatchscript: args.sbatchscript,
                    remote_path: args.remote_path,
                    new_directory: args.new_directory,
                    force: args.force,
                    filters,
                })
            }
            ProjectCmd::List(_args) => ProjectCommand::List(ProjectListCommand),
            ProjectCmd::Check(args) => {
                ProjectCommand::Check(ProjectCheckCommand { name: args.name })
            }
            ProjectCmd::Delete(args) => ProjectCommand::Delete(ProjectDeleteCommand {
                name: args.name,
                yes: args.yes,
            }),
        }),
        Cmd::Completions(_) => {
            unreachable!("completions handled before dispatcher")
        }
    }
}
