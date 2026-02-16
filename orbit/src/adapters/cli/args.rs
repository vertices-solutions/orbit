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
        help = "Path to a TOML config file. When omitted, orbit uses ORBIT_CONFIG_PATH if set, otherwise the default config file location if available."
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
    /// Run either a local directory or a versioned blueprint.
    Run(RunArgs),
    /// Operations on jobs: run jobs, inspect status, and retrieve outputs/results.
    Job(JobArgs),
    /// Operations on clusters: add, delete, poll, and manage clusters.
    Cluster(ClusterArgs),
    /// Operations on local blueprints and Orbitfile metadata.
    Blueprint(BlueprintArgs),
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

#[derive(Args, Debug)]
pub struct RunArgs {
    /// Path to a local directory, or blueprint ref <name:tag>.
    /// Orbit resolves directories first; if not a directory, it resolves as a blueprint.
    pub target: String,
    /// Cluster name.
    #[arg(long = "on", value_name = "CLUSTER")]
    pub cluster: Option<String>,
    /// Path to the sbatch script to run.
    #[arg(long, value_name = "PATH")]
    pub sbatchscript: Option<String>,
    /// Apply a template preset before prompting for missing fields.
    #[arg(long)]
    pub preset: Option<String>,
    /// Template field override in KEY=VALUE form (repeatable).
    #[arg(long, value_name = "KEY=VALUE", action = clap::ArgAction::Append)]
    pub field: Vec<String>,
    /// Accept default template values without prompting (interactive mode only).
    #[arg(long)]
    pub fill_defaults: bool,
    #[arg(long)]
    pub remote_path: Option<String>,
    /// Always create a new remote directory, even if this local path was run before.
    #[arg(long)]
    pub new_directory: bool,
    /// Include paths matching PATTERN.
    #[arg(long, value_name = "PATTERN", action = clap::ArgAction::Append)]
    pub include: Vec<String>,
    /// Exclude paths matching PATTERN.
    #[arg(long, value_name = "PATTERN", action = clap::ArgAction::Append)]
    pub exclude: Vec<String>,
}

#[derive(Subcommand, Debug)]
pub enum JobCmd {
    /// Run a local directory on a cluster.
    Run(JobRunArgs),
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
    #[arg(long)]
    pub blueprint: Option<String>,
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
pub struct ListClustersArgs {
    /// Perform reachability checks and include the reachable column.
    #[arg(long)]
    pub check_reachability: bool,
}

#[derive(Args, Debug)]
pub struct ClusterArgs {
    #[command(subcommand)]
    pub cmd: ClusterCmd,
}

#[derive(Args, Debug)]
pub struct BlueprintArgs {
    #[command(subcommand)]
    pub cmd: BlueprintCmd,
}

#[derive(Subcommand, Debug)]
pub enum BlueprintCmd {
    /// Initialize a blueprint root and Orbitfile.
    Init(BlueprintInitArgs),
    /// Build a blueprint tarball and register it locally.
    Build(BlueprintBuildArgs),
    /// Run a registered blueprint by blueprint name.
    Run(BlueprintRunArgs),
    /// List registered blueprints.
    List(BlueprintListArgs),
    /// Delete a blueprint from the local registry.
    Delete(BlueprintDeleteArgs),
}

#[derive(Args, Debug)]
pub struct BlueprintInitArgs {
    pub path: PathBuf,
    /// Blueprint name stored in Orbitfile and the local registry.
    #[arg(long)]
    pub name: Option<String>,
}

#[derive(Args, Debug)]
pub struct BlueprintBuildArgs {
    pub path: PathBuf,
    /// Include .git directory in the tarball.
    #[arg(long)]
    pub package_git: bool,
}

#[derive(Args, Debug)]
pub struct BlueprintRunArgs {
    /// Built blueprint name:tag (e.g., my-blueprint:20260112.001 or my-blueprint:latest).
    pub blueprint: String,
    /// Cluster name.
    #[arg(long = "on", value_name = "CLUSTER")]
    pub cluster: Option<String>,
    /// Path to the sbatch script to run.
    #[arg(long, value_name = "PATH")]
    pub sbatchscript: Option<String>,
    /// Apply a template preset before prompting for missing fields.
    #[arg(long)]
    pub preset: Option<String>,
    /// Template field override in KEY=VALUE form (repeatable).
    #[arg(long, value_name = "KEY=VALUE", action = clap::ArgAction::Append)]
    pub field: Vec<String>,
    /// Accept default template values without prompting (interactive mode only).
    #[arg(long)]
    pub fill_defaults: bool,
    #[arg(long)]
    pub remote_path: Option<String>,
    /// Always create a new remote directory, even if this local path was run before.
    #[arg(long)]
    pub new_directory: bool,
    /// Include paths matching PATTERN.
    #[arg(long, value_name = "PATTERN", action = clap::ArgAction::Append)]
    pub include: Vec<String>,
    /// Exclude paths matching PATTERN.
    #[arg(long, value_name = "PATTERN", action = clap::ArgAction::Append)]
    pub exclude: Vec<String>,
}

#[derive(Args, Debug)]
pub struct BlueprintListArgs {}

#[derive(Args, Debug)]
pub struct BlueprintDeleteArgs {
    /// Blueprint name or name:tag.
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
    pub name: Option<String>,
}

#[derive(Args, Debug)]
pub struct ClusterLsArgs {
    pub name: Option<String>,
    /// Path to list (absolute or relative to the default base path).
    pub path: Option<String>,
}

#[derive(Args, Debug)]
pub struct SetClusterArgs {
    pub name: Option<String>,

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
    /// Force delete the cluster even if it has running jobs.
    #[arg(long)]
    pub force: bool,
}

#[derive(Args, Debug)]
pub struct JobRunArgs {
    pub local_path: String,
    /// Cluster name.
    #[arg(long = "on", value_name = "CLUSTER")]
    pub cluster: Option<String>,
    /// Path to the sbatch script to run.
    #[arg(long, value_name = "PATH")]
    pub sbatchscript: Option<String>,
    /// Apply a template preset before prompting for missing fields.
    #[arg(long)]
    pub preset: Option<String>,
    /// Template field override in KEY=VALUE form (repeatable).
    #[arg(long, value_name = "KEY=VALUE", action = clap::ArgAction::Append)]
    pub field: Vec<String>,
    /// Accept default template values without prompting (interactive mode only).
    #[arg(long)]
    pub fill_defaults: bool,
    #[arg(long)]
    pub remote_path: Option<String>,
    /// Always create a new remote directory, even if this local path was run before.
    #[arg(long)]
    pub new_directory: bool,
    /// Include paths matching PATTERN.
    /// Rules are checked in the order they appear across --include/--exclude;
    /// the first match wins, and unmatched paths are included.
    /// Patterns match the path relative to the run root with '/' separators.
    /// A pattern without '/' matches the basename anywhere; a pattern with '/'
    /// but no leading '/' is treated as `**/PATTERN`.
    /// A leading '/' anchors to the root, and a trailing '/' matches directories
    /// only (and prunes their contents). Globs support `*`, `?`, `**`, `[]`, `{}`.
    #[arg(long, value_name = "PATTERN", action = clap::ArgAction::Append)]
    pub include: Vec<String>,
    /// Exclude paths matching PATTERN.
    /// Rules are checked in the order they appear across --include/--exclude;
    /// the first match wins, and unmatched paths are included.
    /// Patterns match the path relative to the run root with '/' separators.
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

    /// Mark this cluster as the default cluster.
    #[arg(long = "default")]
    pub is_default: bool,
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
    fn blueprint_delete_yes_defaults_to_false() {
        let args = Cli::parse_from(["orbit", "blueprint", "delete", "proj-a"]);
        match args.cmd {
            Cmd::Blueprint(blueprint) => match blueprint.cmd {
                BlueprintCmd::Delete(delete) => assert!(!delete.yes),
                _ => panic!("expected blueprint delete command"),
            },
            _ => panic!("expected blueprint command"),
        }
    }

    #[test]
    fn blueprint_delete_yes_sets_true() {
        let args = Cli::parse_from(["orbit", "blueprint", "delete", "proj-a", "--yes"]);
        match args.cmd {
            Cmd::Blueprint(blueprint) => match blueprint.cmd {
                BlueprintCmd::Delete(delete) => assert!(delete.yes),
                _ => panic!("expected blueprint delete command"),
            },
            _ => panic!("expected blueprint command"),
        }
    }

    #[test]
    fn cluster_delete_force_defaults_to_false() {
        let args = Cli::parse_from(["orbit", "cluster", "delete", "cluster-a"]);
        match args.cmd {
            Cmd::Cluster(cluster) => match cluster.cmd {
                ClusterCmd::Delete(delete) => {
                    assert_eq!(delete.name, "cluster-a");
                    assert!(!delete.force);
                }
                _ => panic!("expected cluster delete command"),
            },
            _ => panic!("expected cluster command"),
        }
    }

    #[test]
    fn cluster_delete_force_sets_true() {
        let args = Cli::parse_from(["orbit", "cluster", "delete", "cluster-a", "--force"]);
        match args.cmd {
            Cmd::Cluster(cluster) => match cluster.cmd {
                ClusterCmd::Delete(delete) => {
                    assert_eq!(delete.name, "cluster-a");
                    assert!(delete.force);
                }
                _ => panic!("expected cluster delete command"),
            },
            _ => panic!("expected cluster command"),
        }
    }

    #[test]
    fn cluster_list_check_reachability_defaults_to_false() {
        let args = Cli::parse_from(["orbit", "cluster", "list"]);
        match args.cmd {
            Cmd::Cluster(cluster) => match cluster.cmd {
                ClusterCmd::List(list) => assert!(!list.check_reachability),
                _ => panic!("expected cluster list command"),
            },
            _ => panic!("expected cluster command"),
        }
    }

    #[test]
    fn cluster_list_check_reachability_sets_true() {
        let args = Cli::parse_from(["orbit", "cluster", "list", "--check-reachability"]);
        match args.cmd {
            Cmd::Cluster(cluster) => match cluster.cmd {
                ClusterCmd::List(list) => assert!(list.check_reachability),
                _ => panic!("expected cluster list command"),
            },
            _ => panic!("expected cluster command"),
        }
    }

    #[test]
    fn cluster_get_name_is_optional() {
        let args = Cli::parse_from(["orbit", "cluster", "get"]);
        match args.cmd {
            Cmd::Cluster(cluster) => match cluster.cmd {
                ClusterCmd::Get(get) => assert!(get.name.is_none()),
                _ => panic!("expected cluster get command"),
            },
            _ => panic!("expected cluster command"),
        }
    }

    #[test]
    fn cluster_ls_name_is_optional() {
        let args = Cli::parse_from(["orbit", "cluster", "ls"]);
        match args.cmd {
            Cmd::Cluster(cluster) => match cluster.cmd {
                ClusterCmd::Ls(listing) => {
                    assert!(listing.name.is_none());
                    assert!(listing.path.is_none());
                }
                _ => panic!("expected cluster ls command"),
            },
            _ => panic!("expected cluster command"),
        }
    }

    #[test]
    fn cluster_ls_parses_name_and_path() {
        let args = Cli::parse_from(["orbit", "cluster", "ls", "cluster-a", "/tmp"]);
        match args.cmd {
            Cmd::Cluster(cluster) => match cluster.cmd {
                ClusterCmd::Ls(listing) => {
                    assert_eq!(listing.name.as_deref(), Some("cluster-a"));
                    assert_eq!(listing.path.as_deref(), Some("/tmp"));
                }
                _ => panic!("expected cluster ls command"),
            },
            _ => panic!("expected cluster command"),
        }
    }

    #[test]
    fn cluster_set_name_is_optional() {
        let args = Cli::parse_from(["orbit", "cluster", "set", "--host", "node"]);
        match args.cmd {
            Cmd::Cluster(cluster) => match cluster.cmd {
                ClusterCmd::Set(set) => {
                    assert!(set.name.is_none());
                    assert_eq!(set.host.as_deref(), Some("node"));
                }
                _ => panic!("expected cluster set command"),
            },
            _ => panic!("expected cluster command"),
        }
    }

    #[test]
    fn cluster_delete_requires_name() {
        let args = Cli::try_parse_from(["orbit", "cluster", "delete", "--force"]);
        assert!(args.is_err());
    }

    #[test]
    fn cluster_add_default_defaults_to_false() {
        let args = Cli::parse_from(["orbit", "cluster", "add", "alice@node"]);
        match args.cmd {
            Cmd::Cluster(cluster) => match cluster.cmd {
                ClusterCmd::Add(add) => assert!(!add.is_default),
                _ => panic!("expected cluster add command"),
            },
            _ => panic!("expected cluster command"),
        }
    }

    #[test]
    fn cluster_add_default_sets_true() {
        let args = Cli::parse_from(["orbit", "cluster", "add", "alice@node", "--default"]);
        match args.cmd {
            Cmd::Cluster(cluster) => match cluster.cmd {
                ClusterCmd::Add(add) => assert!(add.is_default),
                _ => panic!("expected cluster add command"),
            },
            _ => panic!("expected cluster command"),
        }
    }

    #[test]
    fn job_list_blueprint_defaults_to_none() {
        let args = Cli::parse_from(["orbit", "job", "list"]);
        match args.cmd {
            Cmd::Job(job) => match job.cmd {
                JobCmd::List(list) => assert!(list.blueprint.is_none()),
                _ => panic!("expected job list command"),
            },
            _ => panic!("expected job command"),
        }
    }

    #[test]
    fn job_list_parses_blueprint_flag() {
        let args = Cli::parse_from(["orbit", "job", "list", "--blueprint", "demo"]);
        match args.cmd {
            Cmd::Job(job) => match job.cmd {
                JobCmd::List(list) => assert_eq!(list.blueprint.as_deref(), Some("demo")),
                _ => panic!("expected job list command"),
            },
            _ => panic!("expected job command"),
        }
    }

    #[test]
    fn job_run_parses_cluster_from_on_flag() {
        let args = Cli::parse_from(["orbit", "job", "run", "--on", "winery", "."]);
        match args.cmd {
            Cmd::Job(job) => match job.cmd {
                JobCmd::Run(submit) => {
                    assert_eq!(submit.cluster.as_deref(), Some("winery"));
                    assert_eq!(submit.local_path, ".");
                }
                _ => panic!("expected job run command"),
            },
            _ => panic!("expected job command"),
        }
    }

    #[test]
    fn job_run_parses_sbatchscript_flag() {
        let args = Cli::parse_from([
            "orbit",
            "job",
            "run",
            "--on",
            "winery",
            "--sbatchscript",
            "scripts/submit.sbatch",
            ".",
        ]);
        match args.cmd {
            Cmd::Job(job) => match job.cmd {
                JobCmd::Run(submit) => {
                    assert_eq!(submit.cluster.as_deref(), Some("winery"));
                    assert_eq!(
                        submit.sbatchscript.as_deref(),
                        Some("scripts/submit.sbatch")
                    );
                    assert_eq!(submit.local_path, ".");
                }
                _ => panic!("expected job run command"),
            },
            _ => panic!("expected job command"),
        }
    }

    #[test]
    fn job_run_on_flag_is_optional() {
        let args = Cli::parse_from(["orbit", "job", "run", "."]);
        match args.cmd {
            Cmd::Job(job) => match job.cmd {
                JobCmd::Run(submit) => {
                    assert!(submit.cluster.is_none());
                    assert_eq!(submit.local_path, ".");
                }
                _ => panic!("expected job run command"),
            },
            _ => panic!("expected job command"),
        }
    }

    #[test]
    fn blueprint_run_parses_cluster_from_on_flag() {
        let args = Cli::parse_from(["orbit", "blueprint", "run", "demo:latest", "--on", "winery"]);
        match args.cmd {
            Cmd::Blueprint(blueprint) => match blueprint.cmd {
                BlueprintCmd::Run(submit) => {
                    assert_eq!(submit.blueprint, "demo:latest");
                    assert_eq!(submit.cluster.as_deref(), Some("winery"));
                }
                _ => panic!("expected blueprint run command"),
            },
            _ => panic!("expected blueprint command"),
        }
    }

    #[test]
    fn blueprint_run_parses_sbatchscript_flag() {
        let args = Cli::parse_from([
            "orbit",
            "blueprint",
            "run",
            "demo:latest",
            "--on",
            "winery",
            "--sbatchscript",
            "submit.sbatch",
        ]);
        match args.cmd {
            Cmd::Blueprint(blueprint) => match blueprint.cmd {
                BlueprintCmd::Run(submit) => {
                    assert_eq!(submit.blueprint, "demo:latest");
                    assert_eq!(submit.cluster.as_deref(), Some("winery"));
                    assert_eq!(submit.sbatchscript.as_deref(), Some("submit.sbatch"));
                }
                _ => panic!("expected blueprint run command"),
            },
            _ => panic!("expected blueprint command"),
        }
    }

    #[test]
    fn blueprint_run_on_flag_is_optional() {
        let args = Cli::parse_from(["orbit", "blueprint", "run", "demo:latest"]);
        match args.cmd {
            Cmd::Blueprint(blueprint) => match blueprint.cmd {
                BlueprintCmd::Run(submit) => {
                    assert_eq!(submit.blueprint, "demo:latest");
                    assert!(submit.cluster.is_none());
                }
                _ => panic!("expected blueprint run command"),
            },
            _ => panic!("expected blueprint command"),
        }
    }
}
