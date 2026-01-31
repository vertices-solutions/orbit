// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

use clap::ArgMatches;

use crate::app::commands::*;
use crate::args::{Cli, Cmd, ClusterCmd, JobCmd};
use crate::filters::submit_filters_from_matches;

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
        Cmd::Completions(_) => {
            unreachable!("completions handled before dispatcher")
        }
    }
}
