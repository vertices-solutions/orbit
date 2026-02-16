// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

use std::path::{Path, PathBuf};

use proto::{ListClustersUnitResponse, ListJobsUnitResponse, RunResult, RunStatus};

use crate::app::commands::{AddClusterCapture, CommandResult, RunCapture, StreamCapture};
use crate::app::errors::{AppError, AppResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamKind {
    Generic,
    Run,
}

pub trait PromptFeedbackPort: Send {
    fn start_information_gathering(&mut self, _message: &str) -> AppResult<()> {
        Ok(())
    }
    fn stop_information_gathering(&mut self) -> AppResult<()> {
        Ok(())
    }
    fn start_validation(&mut self, message: &str) -> AppResult<()>;
    fn stop_validation(&mut self) -> AppResult<()>;
    fn finish_success(&mut self, message: &str) -> AppResult<()>;
    fn finish_failure(&mut self, message: &str) -> AppResult<()>;
}

pub struct PromptLine {
    pub input: String,
    feedback: Option<Box<dyn PromptFeedbackPort>>,
}

impl PromptLine {
    pub fn new(input: String, feedback: Option<Box<dyn PromptFeedbackPort>>) -> Self {
        Self { input, feedback }
    }

    pub fn start_validation(&mut self, message: &str) -> AppResult<()> {
        if let Some(feedback) = self.feedback.as_mut() {
            feedback.start_validation(message)?;
        }
        Ok(())
    }

    pub fn stop_validation(&mut self) -> AppResult<()> {
        if let Some(feedback) = self.feedback.as_mut() {
            feedback.stop_validation()?;
        }
        Ok(())
    }

    pub fn finish_success(&mut self, message: &str) -> AppResult<()> {
        if let Some(feedback) = self.feedback.as_mut() {
            feedback.finish_success(message)?;
        }
        Ok(())
    }

    pub fn finish_failure(&mut self, message: &str) -> AppResult<()> {
        if let Some(feedback) = self.feedback.as_mut() {
            feedback.finish_failure(message)?;
        }
        Ok(())
    }
}

#[tonic::async_trait]
pub trait OrbitdPort: Send + Sync {
    async fn ping(&self) -> AppResult<()>;
    async fn list_clusters(
        &self,
        filter: &str,
        check_reachability: bool,
    ) -> AppResult<Vec<ListClustersUnitResponse>>;
    async fn list_jobs(
        &self,
        cluster: Option<String>,
        blueprint: Option<String>,
    ) -> AppResult<Vec<ListJobsUnitResponse>>;
    async fn list_partitions(&self, name: &str) -> AppResult<Vec<String>>;
    async fn list_accounts(&self, name: &str) -> AppResult<Vec<String>>;
    async fn upsert_blueprint(&self, name: &str, path: &str) -> AppResult<proto::BlueprintRecord>;
    async fn get_blueprint(&self, name: &str) -> AppResult<proto::BlueprintRecord>;
    async fn list_blueprints(&self) -> AppResult<Vec<proto::BlueprintRecord>>;
    async fn delete_blueprint(&self, name: &str) -> AppResult<bool>;
    async fn delete_cluster(&self, name: &str, force: bool) -> AppResult<bool>;

    async fn ls(
        &self,
        name: String,
        job_id: Option<i64>,
        path: Option<String>,
        output: &mut dyn StreamOutputPort,
        interaction: &dyn InteractionPort,
    ) -> AppResult<StreamCapture>;

    async fn job_logs(
        &self,
        job_id: i64,
        stderr: bool,
        output: &mut dyn StreamOutputPort,
        interaction: &dyn InteractionPort,
    ) -> AppResult<StreamCapture>;

    async fn job_cancel(
        &self,
        job_id: i64,
        output: &mut dyn StreamOutputPort,
        interaction: &dyn InteractionPort,
    ) -> AppResult<StreamCapture>;

    async fn job_cleanup(
        &self,
        job_id: i64,
        force: bool,
        full: bool,
        output: &mut dyn StreamOutputPort,
        interaction: &dyn InteractionPort,
    ) -> AppResult<StreamCapture>;

    async fn job_retrieve(
        &self,
        job_id: i64,
        path: Option<String>,
        output: Option<PathBuf>,
        overwrite: bool,
        force: bool,
        output_port: &mut dyn StreamOutputPort,
        interaction: &dyn InteractionPort,
    ) -> AppResult<(PathBuf, StreamCapture)>;

    async fn run_job(
        &self,
        name: String,
        local_path: String,
        remote_path: Option<String>,
        new_directory: bool,
        force: bool,
        sbatchscript: String,
        filters: Vec<proto::RunPathFilterRule>,
        blueprint_name: Option<String>,
        default_retrieve_path: Option<String>,
        template_values_json: Option<String>,
        output: &mut dyn StreamOutputPort,
        interaction: &dyn InteractionPort,
    ) -> AppResult<RunCapture>;

    async fn run_blueprint(
        &self,
        blueprint_name: String,
        blueprint_tag: String,
        name: String,
        remote_path: Option<String>,
        new_directory: bool,
        force: bool,
        sbatchscript: String,
        filters: Vec<proto::RunPathFilterRule>,
        default_retrieve_path: Option<String>,
        template_values_json: Option<String>,
        output: &mut dyn StreamOutputPort,
        interaction: &dyn InteractionPort,
    ) -> AppResult<RunCapture>;

    async fn build_blueprint(
        &self,
        path: String,
        package_git: bool,
    ) -> AppResult<proto::BlueprintRecord>;

    async fn add_cluster(
        &self,
        name: String,
        username: String,
        hostname: Option<String>,
        ip: Option<String>,
        identity_path: Option<String>,
        port: u32,
        default_base_path: Option<String>,
        default_scratch_directory: Option<String>,
        interactive_scratch_selection: bool,
        planned_is_default: Option<bool>,
        output: &mut dyn StreamOutputPort,
        interaction: &dyn InteractionPort,
    ) -> AppResult<AddClusterCapture>;

    async fn set_cluster(
        &self,
        name: String,
        host: Option<String>,
        username: Option<String>,
        identity_path: Option<String>,
        port: Option<u32>,
        default_base_path: Option<String>,
        output: &mut dyn StreamOutputPort,
        interaction: &dyn InteractionPort,
    ) -> AppResult<StreamCapture>;

    async fn resolve_home_dir(
        &self,
        name: Option<String>,
        username: String,
        hostname: Option<String>,
        ip: Option<String>,
        identity_path: Option<String>,
        port: u32,
        interaction: &dyn InteractionPort,
    ) -> AppResult<String>;
}

#[tonic::async_trait]
pub trait StreamOutputPort: Send {
    async fn on_stdout(&mut self, bytes: &[u8]) -> AppResult<()>;
    async fn on_stderr(&mut self, bytes: &[u8]) -> AppResult<()>;
    async fn on_exit_code(&mut self, code: i32) -> AppResult<()>;
    async fn on_error(&mut self, code: &str) -> AppResult<()>;
    async fn on_run_status(&mut self, status: &RunStatus) -> AppResult<()>;
    async fn on_run_result(&mut self, result: &RunResult) -> AppResult<()>;

    fn take_stream_capture(&mut self) -> StreamCapture;
    fn take_run_capture(&mut self) -> RunCapture;
}

#[tonic::async_trait]
pub trait InteractionPort: Send + Sync {
    async fn confirm(&self, prompt: &str, help: &str) -> AppResult<bool>;
    async fn prompt_line(&self, prompt: &str, help: &str) -> AppResult<String>;
    async fn prompt_line_with_default(
        &self,
        prompt: &str,
        help: &str,
        default: &str,
    ) -> AppResult<String>;
    async fn prompt_line_confirmable(&self, prompt: &str, help: &str) -> AppResult<PromptLine>;
    async fn prompt_line_with_default_confirmable(
        &self,
        prompt: &str,
        help: &str,
        default: &str,
    ) -> AppResult<PromptLine>;
    async fn prompt_feedback(&self) -> AppResult<Box<dyn PromptFeedbackPort>>;
    async fn select_sbatch(&self, options: &[String]) -> AppResult<Option<String>>;
    async fn select_enum(
        &self,
        name: &str,
        options: &[String],
        default: Option<&str>,
        help: &str,
    ) -> AppResult<String>;
    async fn prompt_mfa(&self, mfa: &proto::MfaPrompt) -> AppResult<proto::MfaAnswer>;
    async fn prompt_mfa_transient(
        &self,
        mfa: &proto::MfaPrompt,
    ) -> AppResult<(proto::MfaAnswer, usize)>;
    async fn clear_transient(&self, lines: usize) -> AppResult<()>;
}

#[tonic::async_trait]
pub trait OutputPort: Send + Sync {
    async fn render(&self, result: &CommandResult) -> AppResult<()>;
    async fn render_error(&self, error: &AppError) -> AppResult<()>;
    async fn info(&self, message: &str) -> AppResult<()>;
    async fn warn(&self, message: &str) -> AppResult<()>;
    async fn success(&self, message: &str) -> AppResult<()>;
    fn stream_output(&self, kind: StreamKind) -> Box<dyn StreamOutputPort>;
}

pub trait FilesystemPort: Send + Sync {
    fn canonicalize(&self, path: &Path) -> AppResult<PathBuf>;
    fn current_dir(&self) -> AppResult<PathBuf>;
    fn read_dir(&self, path: &Path) -> AppResult<Vec<PathBuf>>;
    fn is_file(&self, path: &Path) -> AppResult<bool>;
    fn is_dir(&self, path: &Path) -> AppResult<bool>;
    fn read_file(&self, path: &Path) -> AppResult<Vec<u8>>;
}

pub trait ConfigPort: Send + Sync {
    fn daemon_endpoint(&self, config_path: Option<PathBuf>) -> AppResult<String>;
}

pub trait NetworkPort: Send + Sync {
    fn check_reachable(&self, host: &str, port: u16) -> AppResult<()>;
}
