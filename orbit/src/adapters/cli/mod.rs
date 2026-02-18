// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

mod args;
mod filters;

pub use args::{Cli, Cmd};

use clap::{ArgMatches, CommandFactory};
use clap_complete::{Shell, generate};

use crate::app::commands::*;
use args::{BlueprintCmd, ClusterCmd, JobCmd};
use filters::run_filters_from_matches;

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
        Cmd::Run(args) => {
            let filters = run_filters_from_matches(matches);
            Command::Run(RunCommand {
                target: args.target,
                cluster: args.cluster,
                sbatchscript: args.sbatchscript,
                remote_path: args.remote_path,
                new_directory: args.new_directory,
                filters,
                template_preset: args.preset,
                template_fields: args.field,
                fill_defaults: args.fill_defaults,
            })
        }
        Cmd::Job(job_args) => Command::Job(match job_args.cmd {
            JobCmd::Run(args) => {
                let filters = run_filters_from_matches(matches);
                JobCommand::Run(JobRunCommand {
                    cluster: args.cluster,
                    local_path: args.local_path,
                    sbatchscript: args.sbatchscript,
                    remote_path: args.remote_path,
                    new_directory: args.new_directory,
                    filters,
                    template_preset: args.preset,
                    template_fields: args.field,
                    fill_defaults: args.fill_defaults,
                })
            }
            JobCmd::List(args) => JobCommand::List(ListJobsCommand {
                cluster: args.cluster,
                blueprint: args.blueprint,
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
            ClusterCmd::List(args) => ClusterCommand::List(ListClustersCommand {
                check_reachability: args.check_reachability,
            }),
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
                is_default: args.is_default,
            }),
            ClusterCmd::Set(args) => ClusterCommand::Set(SetClusterCommand {
                cluster: args.cluster,
                setting: args.setting,
            }),
            ClusterCmd::Connect(args) => {
                ClusterCommand::Connect(ConnectClusterCommand { name: args.name })
            }
            ClusterCmd::Delete(args) => ClusterCommand::Delete(DeleteClusterCommand {
                name: args.name,
                yes: args.yes,
                force: args.force,
            }),
        }),
        Cmd::Blueprint(project_args) => Command::Blueprint(match project_args.cmd {
            BlueprintCmd::Init(args) => BlueprintCommand::Init(BlueprintInitCommand {
                path: args.path,
                name: args.name,
            }),
            BlueprintCmd::Build(args) => BlueprintCommand::Build(BlueprintBuildCommand {
                path: args.path,
                package_git: args.package_git,
            }),
            BlueprintCmd::Run(args) => {
                let filters = run_filters_from_matches(matches);
                BlueprintCommand::Run(BlueprintRunCommand {
                    blueprint: args.blueprint,
                    cluster: args.cluster,
                    sbatchscript: args.sbatchscript,
                    remote_path: args.remote_path,
                    new_directory: args.new_directory,
                    filters,
                    template_preset: args.preset,
                    template_fields: args.field,
                    fill_defaults: args.fill_defaults,
                })
            }
            BlueprintCmd::List(_args) => BlueprintCommand::List(BlueprintListCommand),
            BlueprintCmd::Delete(args) => BlueprintCommand::Delete(BlueprintDeleteCommand {
                name: args.name,
                yes: args.yes,
            }),
        }),
        Cmd::Completions(_) => {
            unreachable!("completions handled before dispatcher")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::FromArgMatches;

    #[test]
    fn command_from_cli_maps_job_list_blueprint_filter() {
        let command = cli_command();
        let matches = command
            .try_get_matches_from([
                "orbit",
                "job",
                "list",
                "--cluster",
                "cluster-a",
                "--blueprint",
                "demo-blueprint",
            ])
            .expect("parse matches");
        let cli = Cli::from_arg_matches(&matches).expect("parse cli");

        let command = command_from_cli(cli, &matches);
        match command {
            Command::Job(JobCommand::List(list)) => {
                assert_eq!(list.cluster.as_deref(), Some("cluster-a"));
                assert_eq!(list.blueprint.as_deref(), Some("demo-blueprint"));
            }
            _ => panic!("expected job list command"),
        }
    }

    #[test]
    fn command_from_cli_maps_job_list_without_blueprint_filter() {
        let command = cli_command();
        let matches = command
            .try_get_matches_from(["orbit", "job", "list", "--cluster", "cluster-a"])
            .expect("parse matches");
        let cli = Cli::from_arg_matches(&matches).expect("parse cli");

        let command = command_from_cli(cli, &matches);
        match command {
            Command::Job(JobCommand::List(list)) => {
                assert_eq!(list.cluster.as_deref(), Some("cluster-a"));
                assert!(list.blueprint.is_none());
            }
            _ => panic!("expected job list command"),
        }
    }

    #[test]
    fn command_from_cli_maps_cluster_set_setting() {
        let command = cli_command();
        let matches = command
            .try_get_matches_from([
                "orbit",
                "cluster",
                "set",
                "--on",
                "cluster-a",
                "default=true",
            ])
            .expect("parse matches");
        let cli = Cli::from_arg_matches(&matches).expect("parse cli");

        let command = command_from_cli(cli, &matches);
        match command {
            Command::Cluster(ClusterCommand::Set(set)) => {
                assert_eq!(set.cluster, "cluster-a");
                assert_eq!(set.setting, "default=true");
            }
            _ => panic!("expected cluster set command"),
        }
    }

    #[test]
    fn command_from_cli_maps_cluster_connect_name() {
        let command = cli_command();
        let matches = command
            .try_get_matches_from(["orbit", "cluster", "connect", "cluster-a"])
            .expect("parse matches");
        let cli = Cli::from_arg_matches(&matches).expect("parse cli");

        let command = command_from_cli(cli, &matches);
        match command {
            Command::Cluster(ClusterCommand::Connect(connect)) => {
                assert_eq!(connect.name, "cluster-a");
            }
            _ => panic!("expected cluster connect command"),
        }
    }
}
