// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

use std::path::PathBuf;

use proto::SubmitPathFilterRule;

mod results;

pub use results::{CommandResult, InitActionStatus, ProjectInitAction, StreamCapture, SubmitCapture};

#[derive(Debug, Clone)]
pub enum Command {
    Ping(PingCommand),
    Job(JobCommand),
    Cluster(ClusterCommand),
    Project(ProjectCommand),
}

#[derive(Debug, Clone)]
pub struct PingCommand;

#[derive(Debug, Clone)]
pub enum JobCommand {
    Submit(SubmitJobCommand),
    List(ListJobsCommand),
    Get(JobGetCommand),
    Logs(JobLogsCommand),
    Cancel(JobCancelCommand),
    Cleanup(JobCleanupCommand),
    Ls(JobLsCommand),
    Retrieve(JobRetrieveCommand),
}

#[derive(Debug, Clone)]
pub struct SubmitJobCommand {
    pub name: String,
    pub local_path: String,
    pub sbatchscript: Option<String>,
    pub remote_path: Option<String>,
    pub new_directory: bool,
    pub force: bool,
    pub filters: Vec<SubmitPathFilterRule>,
    pub template_preset: Option<String>,
    pub template_fields: Vec<String>,
    pub fill_defaults: bool,
}

#[derive(Debug, Clone)]
pub struct ListJobsCommand {
    pub cluster: Option<String>,
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
pub struct ListClustersCommand;

#[derive(Debug, Clone)]
pub struct ClusterGetCommand {
    pub name: String,
}

#[derive(Debug, Clone)]
pub struct ClusterLsCommand {
    pub name: String,
    pub path: Option<String>,
}

#[derive(Debug, Clone)]
pub struct AddClusterCommand {
    pub destination: Option<String>,
    pub name: Option<String>,
    pub identity_path: Option<String>,
    pub default_base_path: Option<String>,
}

#[derive(Debug, Clone)]
pub struct SetClusterCommand {
    pub name: String,
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
}

#[derive(Debug, Clone)]
pub enum ProjectCommand {
    Init(ProjectInitCommand),
    Submit(ProjectSubmitCommand),
    List(ProjectListCommand),
    Check(ProjectCheckCommand),
    Delete(ProjectDeleteCommand),
}

#[derive(Debug, Clone)]
pub struct ProjectInitCommand {
    pub path: PathBuf,
    pub name: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ProjectSubmitCommand {
    pub project: String,
    pub cluster: String,
    pub sbatchscript: Option<String>,
    pub remote_path: Option<String>,
    pub new_directory: bool,
    pub force: bool,
    pub filters: Vec<SubmitPathFilterRule>,
    pub template_preset: Option<String>,
    pub template_fields: Vec<String>,
    pub fill_defaults: bool,
}

#[derive(Debug, Clone)]
pub struct ProjectListCommand;

#[derive(Debug, Clone)]
pub struct ProjectCheckCommand {
    pub name: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ProjectDeleteCommand {
    pub name: String,
    pub yes: bool,
}
