// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

use std::path::PathBuf;

use proto::RunPathFilterRule;

mod results;

pub use results::{
    AddClusterCapture, BlueprintInitAction, BlueprintListItem, CommandResult, InitActionStatus,
    RunCapture, StreamCapture,
};

#[derive(Debug, Clone)]
pub enum Command {
    Ping(PingCommand),
    Run(RunCommand),
    Job(JobCommand),
    Cluster(ClusterCommand),
    Blueprint(BlueprintCommand),
}

#[derive(Debug, Clone)]
pub struct PingCommand;

#[derive(Debug, Clone)]
pub enum JobCommand {
    Run(JobRunCommand),
    List(ListJobsCommand),
    Get(JobGetCommand),
    Logs(JobLogsCommand),
    Cancel(JobCancelCommand),
    Cleanup(JobCleanupCommand),
    Ls(JobLsCommand),
    Retrieve(JobRetrieveCommand),
}

#[derive(Debug, Clone)]
pub struct JobRunCommand {
    pub cluster: Option<String>,
    pub local_path: String,
    pub sbatchscript: Option<String>,
    pub remote_path: Option<String>,
    pub new_directory: bool,
    pub filters: Vec<RunPathFilterRule>,
    pub template_preset: Option<String>,
    pub template_fields: Vec<String>,
    pub fill_defaults: bool,
}

#[derive(Debug, Clone)]
pub struct RunCommand {
    pub target: String,
    pub cluster: Option<String>,
    pub sbatchscript: Option<String>,
    pub remote_path: Option<String>,
    pub new_directory: bool,
    pub filters: Vec<RunPathFilterRule>,
    pub template_preset: Option<String>,
    pub template_fields: Vec<String>,
    pub fill_defaults: bool,
}

#[derive(Debug, Clone)]
pub struct ListJobsCommand {
    pub cluster: Option<String>,
    pub blueprint: Option<String>,
}

#[derive(Debug, Clone)]
pub struct JobGetCommand {
    pub job_id: i64,
    pub cluster: Option<String>,
}

#[derive(Debug, Clone)]
pub struct JobLogsCommand {
    pub job_id: i64,
    pub err: bool,
}

#[derive(Debug, Clone)]
pub struct JobCancelCommand {
    pub job_id: i64,
    pub yes: bool,
}

#[derive(Debug, Clone)]
pub struct JobCleanupCommand {
    pub job_id: i64,
    pub force: bool,
    pub full: bool,
    pub yes: bool,
}

#[derive(Debug, Clone)]
pub struct JobLsCommand {
    pub job_id: i64,
    pub path: Option<String>,
    pub cluster: Option<String>,
}

#[derive(Debug, Clone)]
pub struct JobRetrieveCommand {
    pub job_id: i64,
    pub path: Option<String>,
    pub output: Option<PathBuf>,
    pub overwrite: bool,
    pub force: bool,
}

#[derive(Debug, Clone)]
pub enum ClusterCommand {
    List(ListClustersCommand),
    Get(ClusterGetCommand),
    Ls(ClusterLsCommand),
    Add(AddClusterCommand),
    Set(SetClusterCommand),
    Delete(DeleteClusterCommand),
}

#[derive(Debug, Clone)]
pub struct ListClustersCommand {
    pub check_reachability: bool,
}

#[derive(Debug, Clone)]
pub struct ClusterGetCommand {
    pub name: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ClusterLsCommand {
    pub name: Option<String>,
    pub path: Option<String>,
}

#[derive(Debug, Clone)]
pub struct AddClusterCommand {
    pub destination: Option<String>,
    pub name: Option<String>,
    pub identity_path: Option<String>,
    pub default_base_path: Option<String>,
    pub is_default: bool,
}

#[derive(Debug, Clone)]
pub struct SetClusterCommand {
    pub name: Option<String>,
    pub host: Option<String>,
    pub username: Option<String>,
    pub port: Option<u32>,
    pub identity_path: Option<String>,
    pub default_base_path: Option<String>,
}

#[derive(Debug, Clone)]
pub struct DeleteClusterCommand {
    pub name: String,
    pub yes: bool,
    pub force: bool,
}

#[derive(Debug, Clone)]
pub enum BlueprintCommand {
    Init(BlueprintInitCommand),
    Build(BlueprintBuildCommand),
    Run(BlueprintRunCommand),
    List(BlueprintListCommand),
    Delete(BlueprintDeleteCommand),
}

#[derive(Debug, Clone)]
pub struct BlueprintInitCommand {
    pub path: PathBuf,
    pub name: Option<String>,
}

#[derive(Debug, Clone)]
pub struct BlueprintBuildCommand {
    pub path: PathBuf,
    pub package_git: bool,
}

#[derive(Debug, Clone)]
pub struct BlueprintRunCommand {
    pub blueprint: String,
    pub cluster: Option<String>,
    pub sbatchscript: Option<String>,
    pub remote_path: Option<String>,
    pub new_directory: bool,
    pub filters: Vec<RunPathFilterRule>,
    pub template_preset: Option<String>,
    pub template_fields: Vec<String>,
    pub fill_defaults: bool,
}

#[derive(Debug, Clone)]
pub struct BlueprintListCommand;

#[derive(Debug, Clone)]
pub struct BlueprintDeleteCommand {
    pub name: String,
    pub yes: bool,
}
