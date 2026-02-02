// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

use clap::{Args, Parser, Subcommand};
use clap_complete::Shell;
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "orbit", version, about, long_about = None)]
pub struct Cli {
    #[arg(
        short,
        long,
        value_name = "PATH",
        help = "Path to a TOML config file. When omitted, orbit uses the default config file location if available."
    )]
    pub config: Option<PathBuf>,
    #[arg(
        long,
        global = true,
        help = "Run without prompts, fail on MFA, and output JSON only."
    )]
    pub non_interactive: bool,
    #[command(subcommand)]
    pub cmd: Cmd,
}

#[derive(Subcommand)]
pub enum Cmd {
    /// Check that the daemon is reachable.
    Ping,
    /// Operations on jobs: submit job, inspect its status, and retrieve its outputs and results.
    Job(JobArgs),
    /// Operations on clusters: add, delete, poll, and manage clusters.
    Cluster(ClusterArgs),
    /// Operations on local projects and Orbitfile metadata.
    Project(ProjectArgs),
    /// Generate shell completions.
    Completions(CompletionsArgs),
}

#[derive(Args, Debug)]
pub struct CompletionsArgs {
    #[arg(value_enum)]
    pub shell: Shell,
}

#[derive(Args, Debug)]
pub struct JobArgs {
    #[command(subcommand)]
    pub cmd: JobCmd,
}

#[derive(Subcommand, Debug)]
pub enum JobCmd {
    /// Submit a project to a cluster.
    Submit(SubmitArgs),
    /// List jobs.
    List(ListJobsArgs),
    /// Show job details.
    Get(JobGetArgs),
    /// Show job logs.
    Logs(JobLogsArgs),
    /// Cancel a job.
    Cancel(JobCancelArgs),
    /// Clean up a job's remote directory.
    Cleanup(JobCleanupArgs),
    /// List files in a job work directory.
    Ls(JobLsArgs),
    /// Retrieve a file or directory from a job run folder.
    Retrieve(JobRetrieveArgs),
}

#[derive(Args, Debug)]
pub struct JobGetArgs {
    /// Job id from the daemon.
    pub job_id: i64,
    #[arg(long)]
    pub cluster: Option<String>,
}

#[derive(Args, Debug)]
pub struct JobLogsArgs {
    /// Job id from the daemon.
    pub job_id: i64,
    /// Show stderr instead of stdout.
    #[arg(long)]
    pub err: bool,
}

#[derive(Args, Debug)]
pub struct JobCancelArgs {
    /// Job id from the daemon.
    pub job_id: i64,
    /// Skip the confirmation prompt.
    #[arg(long, short = 'y')]
    pub yes: bool,
}

#[derive(Args, Debug)]
pub struct JobCleanupArgs {
    /// Job id from the daemon.
    pub job_id: i64,
    /// Cancel a running job before cleanup.
    #[arg(long)]
    pub force: bool,
    /// Delete the job record from the local database after cleanup.
    #[arg(long)]
    pub full: bool,
    /// Skip the confirmation prompt.
    #[arg(long, short = 'y')]
    pub yes: bool,
}

#[derive(Args, Debug)]
pub struct ListJobsArgs {
    #[arg(long)]
    pub cluster: Option<String>,
}

#[derive(Args, Debug)]
pub struct JobRetrieveArgs {
    /// Job id from the daemon.
    pub job_id: i64,
    /// Optional remote path (absolute or relative to the job run folder).
    pub path: Option<String>,
    #[arg(
        long,
        help = "Directory where the requested file or directory will be placed."
    )]
    pub output: Option<PathBuf>,
    #[arg(long, help = "Overwrite existing local files.")]
    pub overwrite: bool,
    #[arg(long, help = "Retrieve outputs even if the job has not completed.")]
    pub force: bool,
}

#[derive(Args, Debug)]
pub struct JobLsArgs {
    /// Job id from the daemon.
    pub job_id: i64,
    /// Path to list (absolute or relative to the job root).
    pub path: Option<String>,
    #[arg(long)]
    pub cluster: Option<String>,
}

#[derive(Args, Debug)]
pub struct ListClustersArgs {}

#[derive(Args, Debug)]
pub struct ClusterArgs {
    #[command(subcommand)]
    pub cmd: ClusterCmd,
}

#[derive(Args, Debug)]
pub struct ProjectArgs {
    #[command(subcommand)]
    pub cmd: ProjectCmd,
}

#[derive(Subcommand, Debug)]
pub enum ProjectCmd {
    /// Initialize a project root and Orbitfile.
    Init(ProjectInitArgs),
    /// Submit a registered project by project name.
    Submit(ProjectSubmitArgs),
    /// List registered projects.
    List(ProjectListArgs),
    /// Validate one or all registered projects.
    Check(ProjectCheckArgs),
    /// Delete a project from the local registry.
    Delete(ProjectDeleteArgs),
}

#[derive(Args, Debug)]
pub struct ProjectInitArgs {
    pub path: PathBuf,
    /// Project name stored in Orbitfile and the local registry.
    #[arg(long)]
    pub name: Option<String>,
}

#[derive(Args, Debug)]
pub struct ProjectSubmitArgs {
    /// Registered project name.
    pub project: String,
    /// Cluster name.
    pub cluster: String,
    pub sbatchscript: Option<String>,
    #[arg(long)]
    pub remote_path: Option<String>,
    /// Always create a new remote directory, even if this local path was submitted before.
    #[arg(long)]
    pub new_directory: bool,
    /// Allow submitting into a remote directory even if another job is running there.
    #[arg(long)]
    pub force: bool,
    /// Include paths matching PATTERN.
    #[arg(long, value_name = "PATTERN", action = clap::ArgAction::Append)]
    pub include: Vec<String>,
    /// Exclude paths matching PATTERN.
    #[arg(long, value_name = "PATTERN", action = clap::ArgAction::Append)]
    pub exclude: Vec<String>,
}

#[derive(Args, Debug)]
pub struct ProjectListArgs {}

#[derive(Args, Debug)]
pub struct ProjectCheckArgs {
    /// Project name. If omitted, all registered projects are checked.
    pub name: Option<String>,
}

#[derive(Args, Debug)]
pub struct ProjectDeleteArgs {
    /// Registered project name.
    pub name: String,
    /// Skip the confirmation prompt.
    #[arg(long, short = 'y')]
    pub yes: bool,
}

#[derive(Subcommand, Debug)]
pub enum ClusterCmd {
    /// List clusters.
    List(ListClustersArgs),
    /// Show cluster details.
    Get(ClusterGetArgs),
    /// List files on a cluster.
    Ls(ClusterLsArgs),
    /// Add a new cluster.
    Add(AddClusterArgs),
    /// Update cluster parameters.
    Set(SetClusterArgs),
    /// Delete a cluster and its job records.
    Delete(DeleteClusterArgs),
}

#[derive(Args, Debug)]
pub struct ClusterGetArgs {
    pub name: String,
}

#[derive(Args, Debug)]
pub struct ClusterLsArgs {
    pub name: String,
    /// Path to list (absolute or relative to the default base path).
    pub path: Option<String>,
}

#[derive(Args, Debug)]
pub struct SetClusterArgs {
    pub name: String,

    /// Use a remote host (hostname or IP address)
    #[arg(long)]
    pub host: Option<String>,

    /// Use a different username for SSH
    #[arg(long)]
    pub username: Option<String>,

    #[arg(long)]
    pub port: Option<u32>,

    #[arg(long)]
    pub identity_path: Option<String>,

    #[arg(long)]
    pub default_base_path: Option<String>,
}

#[derive(Args, Debug)]
pub struct DeleteClusterArgs {
    pub name: String,
    /// Skip the confirmation prompt.
    #[arg(long, short = 'y')]
    pub yes: bool,
}

#[derive(Args, Debug)]
pub struct SubmitArgs {
    pub name: String,
    pub local_path: String,
    pub sbatchscript: Option<String>,
    #[arg(long)]
    pub remote_path: Option<String>,
    /// Always create a new remote directory, even if this local path was submitted before.
    #[arg(long)]
    pub new_directory: bool,
    /// Allow submitting into a remote directory even if another job is running there.
    #[arg(long)]
    pub force: bool,
    /// Include paths matching PATTERN.
    /// Rules are checked in the order they appear across --include/--exclude;
    /// the first match wins, and unmatched paths are included.
    /// Patterns match the path relative to the submit root with '/' separators.
    /// A pattern without '/' matches the basename anywhere; a pattern with '/'
    /// but no leading '/' is treated as `**/PATTERN`.
    /// A leading '/' anchors to the root, and a trailing '/' matches directories
    /// only (and prunes their contents). Globs support `*`, `?`, `**`, `[]`, `{}`.
    #[arg(long, value_name = "PATTERN", action = clap::ArgAction::Append)]
    pub include: Vec<String>,
    /// Exclude paths matching PATTERN.
    /// Rules are checked in the order they appear across --include/--exclude;
    /// the first match wins, and unmatched paths are included.
    /// Patterns match the path relative to the submit root with '/' separators.
    /// A pattern without '/' matches the basename anywhere; a pattern with '/'
    /// but no leading '/' is treated as `**/PATTERN`.
    /// A leading '/' anchors to the root, and a trailing '/' matches directories
    /// only (and prunes their contents). Globs support `*`, `?`, `**`, `[]`, `{}`.
    #[arg(long, value_name = "PATTERN", action = clap::ArgAction::Append)]
    pub exclude: Vec<String>,
}

#[derive(Args, Debug)]
pub struct AddClusterArgs {
    /// Destination in ssh format: user@host[:port] (required in non-interactive mode)
    #[arg(value_name = "DESTINATION")]
    pub destination: Option<String>,

    /// Friendly cluster name you’ll use in other commands (e.g. "gpu01" or "lab-cluster").
    #[arg(long)]
    pub name: Option<String>,

    /// Defaults to ~/.ssh/id_ed25519.
    #[arg(long)]
    pub identity_path: Option<String>,

    #[arg(long)]
    pub default_base_path: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn job_retrieve_force_defaults_to_false() {
        let args = Cli::parse_from(["orbit", "job", "retrieve", "12", "output.txt"]);
        match args.cmd {
            Cmd::Job(job) => match job.cmd {
                JobCmd::Retrieve(retrieve) => assert!(!retrieve.force),
                _ => panic!("expected retrieve command"),
            },
            _ => panic!("expected job command"),
        }
    }

    #[test]
    fn job_retrieve_force_sets_true() {
        let args = Cli::parse_from(["orbit", "job", "retrieve", "12", "output.txt", "--force"]);
        match args.cmd {
            Cmd::Job(job) => match job.cmd {
                JobCmd::Retrieve(retrieve) => assert!(retrieve.force),
                _ => panic!("expected retrieve command"),
            },
            _ => panic!("expected job command"),
        }
    }

    #[test]
    fn job_cancel_yes_defaults_to_false() {
        let args = Cli::parse_from(["orbit", "job", "cancel", "12"]);
        match args.cmd {
            Cmd::Job(job) => match job.cmd {
                JobCmd::Cancel(cancel) => assert!(!cancel.yes),
                _ => panic!("expected cancel command"),
            },
            _ => panic!("expected job command"),
        }
    }

    #[test]
    fn job_cancel_yes_sets_true() {
        let args = Cli::parse_from(["orbit", "job", "cancel", "12", "--yes"]);
        match args.cmd {
            Cmd::Job(job) => match job.cmd {
                JobCmd::Cancel(cancel) => assert!(cancel.yes),
                _ => panic!("expected cancel command"),
            },
            _ => panic!("expected job command"),
        }
    }

    #[test]
    fn non_interactive_parses_globally() {
        let args = Cli::parse_from(["orbit", "--non-interactive", "ping"]);
        assert!(args.non_interactive);
        assert!(matches!(args.cmd, Cmd::Ping));
    }

    #[test]
    fn job_cleanup_force_defaults_to_false() {
        let args = Cli::parse_from(["orbit", "job", "cleanup", "12"]);
        match args.cmd {
            Cmd::Job(job) => match job.cmd {
                JobCmd::Cleanup(cleanup) => assert!(!cleanup.force),
                _ => panic!("expected cleanup command"),
            },
            _ => panic!("expected job command"),
        }
    }

    #[test]
    fn job_cleanup_force_sets_true() {
        let args = Cli::parse_from(["orbit", "job", "cleanup", "12", "--force"]);
        match args.cmd {
            Cmd::Job(job) => match job.cmd {
                JobCmd::Cleanup(cleanup) => assert!(cleanup.force),
                _ => panic!("expected cleanup command"),
            },
            _ => panic!("expected job command"),
        }
    }

    #[test]
    fn job_cleanup_full_defaults_to_false() {
        let args = Cli::parse_from(["orbit", "job", "cleanup", "12"]);
        match args.cmd {
            Cmd::Job(job) => match job.cmd {
                JobCmd::Cleanup(cleanup) => assert!(!cleanup.full),
                _ => panic!("expected cleanup command"),
            },
            _ => panic!("expected job command"),
        }
    }

    #[test]
    fn job_cleanup_yes_defaults_to_false() {
        let args = Cli::parse_from(["orbit", "job", "cleanup", "12"]);
        match args.cmd {
            Cmd::Job(job) => match job.cmd {
                JobCmd::Cleanup(cleanup) => assert!(!cleanup.yes),
                _ => panic!("expected cleanup command"),
            },
            _ => panic!("expected job command"),
        }
    }

    #[test]
    fn completions_shell_parses() {
        let args = Cli::parse_from(["orbit", "completions", "bash"]);
        match args.cmd {
            Cmd::Completions(completions) => assert!(matches!(completions.shell, Shell::Bash)),
            _ => panic!("expected completions command"),
        }
    }

    #[test]
    fn project_delete_yes_defaults_to_false() {
        let args = Cli::parse_from(["orbit", "project", "delete", "proj-a"]);
        match args.cmd {
            Cmd::Project(project) => match project.cmd {
                ProjectCmd::Delete(delete) => assert!(!delete.yes),
                _ => panic!("expected project delete command"),
            },
            _ => panic!("expected project command"),
        }
    }

    #[test]
    fn project_delete_yes_sets_true() {
        let args = Cli::parse_from(["orbit", "project", "delete", "proj-a", "--yes"]);
        match args.cmd {
            Cmd::Project(project) => match project.cmd {
                ProjectCmd::Delete(delete) => assert!(delete.yes),
                _ => panic!("expected project delete command"),
            },
            _ => panic!("expected project command"),
        }
    }
}
