// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

mod console;
mod format;
mod mfa;
mod prompt;
mod sbatch_picker;

use std::io::Write;

use crate::app::commands::{CommandResult, StreamCapture, SubmitCapture};
use crate::app::errors::{AppError, AppResult, format_server_error};
use crate::app::ports::{InteractionPort, OutputPort, StreamKind, StreamOutputPort};
use console::{print_with_green_check_stderr, print_with_green_check_stdout};
use format::{
    format_cluster_details, format_clusters_table, format_job_details, format_jobs_table,
    format_projects_table,
};
use mfa::{clear_transient_mfa, collect_mfa_answers, collect_mfa_answers_transient};
use sbatch_picker::pick_sbatch_script;

pub struct TerminalOutput;

impl TerminalOutput {
    pub fn new() -> Self {
        Self
    }
}

#[tonic::async_trait]
impl OutputPort for TerminalOutput {
    async fn render(&self, result: &CommandResult) -> AppResult<()> {
        match result {
            CommandResult::Message { message } => {
                println!("{message}");
            }
            CommandResult::Pong { message } => {
                println!("{message}");
            }
            CommandResult::JobList { jobs } => {
                print!("{}", format_jobs_table(jobs));
            }
            CommandResult::JobDetails { job } => {
                print!("{}", format_job_details(job));
            }
            CommandResult::JobSubmit { .. } => {}
            CommandResult::JobLogs { .. } => {}
            CommandResult::JobCancel { .. } => {}
            CommandResult::JobCleanup { .. } => {}
            CommandResult::JobLs { .. } => {}
            CommandResult::JobRetrieve { output, .. } => {
                eprintln!("Wrote to {}", output.display());
            }
            CommandResult::ClusterList { clusters } => {
                print!("{}", format_clusters_table(clusters));
            }
            CommandResult::ClusterDetails { cluster } => {
                print!("{}", format_cluster_details(cluster));
            }
            CommandResult::ClusterLs { .. } => {}
            CommandResult::ClusterAdd { name, .. } => {
                println!("Cluster {} added successfully!", name);
            }
            CommandResult::ClusterSet { updated_fields, .. } => {
                for (key, value) in updated_fields {
                    println!("Set {key}={value} successfully");
                }
            }
            CommandResult::ClusterDelete { name } => {
                println!("Cluster '{}' deleted.", name);
            }
            CommandResult::ProjectInit {
                name,
                path,
                orbitfile,
                git_initialized,
            } => {
                println!("Project '{}' initialized at {}", name, path.display());
                println!("Orbitfile: {}", orbitfile.display());
                if *git_initialized {
                    println!("Initialized git repository.");
                }
            }
            CommandResult::ProjectList { projects } => {
                if projects.is_empty() {
                    println!("No projects registered.");
                } else {
                    print!("{}", format_projects_table(projects));
                }
            }
            CommandResult::ProjectCheck { checked } => {
                println!("{checked} PASSED");
            }
            CommandResult::ProjectDelete { name } => {
                println!("Project '{}' deleted.", name);
            }
        }
        Ok(())
    }

    async fn render_error(&self, error: &AppError) -> AppResult<()> {
        eprintln!("{}", error.message);
        Ok(())
    }

    async fn info(&self, message: &str) -> AppResult<()> {
        println!("{message}");
        Ok(())
    }

    async fn warn(&self, message: &str) -> AppResult<()> {
        eprintln!("{message}");
        Ok(())
    }

    async fn success(&self, message: &str) -> AppResult<()> {
        print_with_green_check_stdout(message).map_err(|err| AppError::local_error(err.to_string()))
    }

    fn stream_output(&self, kind: StreamKind) -> Box<dyn StreamOutputPort> {
        Box::new(TerminalStreamOutput::new(kind))
    }
}

pub struct TerminalInteraction;

impl TerminalInteraction {
    pub fn new() -> Self {
        Self
    }
}

#[tonic::async_trait]
impl InteractionPort for TerminalInteraction {
    async fn confirm(&self, prompt: &str, help: &str) -> AppResult<bool> {
        prompt::confirm_action(prompt, help).map_err(|err| AppError::local_error(err.to_string()))
    }

    async fn prompt_line(&self, prompt: &str, help: &str) -> AppResult<String> {
        prompt::prompt_line(prompt, help).map_err(|err| AppError::local_error(err.to_string()))
    }

    async fn prompt_line_with_default(
        &self,
        prompt: &str,
        help: &str,
        default: &str,
    ) -> AppResult<String> {
        prompt::prompt_line_with_default(prompt, help, Some(default))
            .map_err(|err| AppError::local_error(err.to_string()))
    }

    async fn select_sbatch(&self, options: &[String]) -> AppResult<Option<String>> {
        if options.is_empty() {
            return Ok(None);
        }
        pick_sbatch_script(options.to_vec())
            .map(Some)
            .map_err(|err| AppError::local_error(err.to_string()))
    }

    async fn prompt_mfa(&self, mfa: &proto::MfaPrompt) -> AppResult<proto::MfaAnswer> {
        collect_mfa_answers(mfa)
            .await
            .map_err(|err| AppError::local_error(err.to_string()))
    }

    async fn prompt_mfa_transient(
        &self,
        mfa: &proto::MfaPrompt,
    ) -> AppResult<(proto::MfaAnswer, usize)> {
        collect_mfa_answers_transient(mfa)
            .await
            .map_err(|err| AppError::local_error(err.to_string()))
    }

    async fn clear_transient(&self, lines: usize) -> AppResult<()> {
        clear_transient_mfa(lines).map_err(|err| AppError::local_error(err.to_string()))
    }
}

struct TerminalStreamOutput {
    kind: StreamKind,
    stream: StreamCapture,
    submit: SubmitCapture,
    printed_remote_path: bool,
}

impl TerminalStreamOutput {
    fn new(kind: StreamKind) -> Self {
        Self {
            kind,
            stream: StreamCapture::default(),
            submit: SubmitCapture::default(),
            printed_remote_path: false,
        }
    }

    fn record_submit(&mut self, f: impl FnOnce(&mut SubmitCapture)) {
        if self.kind == StreamKind::Submit {
            f(&mut self.submit);
        }
    }
}

#[tonic::async_trait]
impl StreamOutputPort for TerminalStreamOutput {
    async fn on_stdout(&mut self, bytes: &[u8]) -> AppResult<()> {
        std::io::stdout()
            .write_all(bytes)
            .and_then(|_| std::io::stdout().flush())
            .map_err(|err| AppError::local_error(err.to_string()))?;
        self.stream.stdout.extend_from_slice(bytes);
        self.record_submit(|capture| capture.stdout.extend_from_slice(bytes));
        Ok(())
    }

    async fn on_stderr(&mut self, bytes: &[u8]) -> AppResult<()> {
        std::io::stderr()
            .write_all(bytes)
            .and_then(|_| std::io::stderr().flush())
            .map_err(|err| AppError::local_error(err.to_string()))?;
        self.stream.stderr.extend_from_slice(bytes);
        self.record_submit(|capture| capture.stderr.extend_from_slice(bytes));
        Ok(())
    }

    async fn on_exit_code(&mut self, code: i32) -> AppResult<()> {
        self.stream.exit_code = Some(code);
        self.record_submit(|capture| capture.exit_code = Some(code));
        Ok(())
    }

    async fn on_error(&mut self, code: &str) -> AppResult<()> {
        self.stream.error_code = Some(code.to_string());
        self.record_submit(|capture| capture.error_code = Some(code.to_string()));
        eprintln!("{}", format_server_error(code));
        Ok(())
    }

    async fn on_submit_status(&mut self, status: &proto::SubmitStatus) -> AppResult<()> {
        if self.kind != StreamKind::Submit {
            return Ok(());
        }
        let phase = proto::submit_status::Phase::try_from(status.phase)
            .unwrap_or(proto::submit_status::Phase::Unspecified);
        match phase {
            proto::submit_status::Phase::Resolved => {
                if !status.remote_path.is_empty() && !self.printed_remote_path {
                    print_with_green_check_stdout(&format!("Remote path: {}", status.remote_path))
                        .map_err(|err| AppError::local_error(err.to_string()))?;
                    self.printed_remote_path = true;
                }
                if !status.remote_path.is_empty() {
                    self.submit.remote_path = Some(status.remote_path.clone());
                }
            }
            proto::submit_status::Phase::TransferDone => {
                print_with_green_check_stderr("Data transfer complete.")
                    .map_err(|err| AppError::local_error(err.to_string()))?;
            }
            _ => {}
        }
        Ok(())
    }

    async fn on_submit_result(&mut self, result: &proto::SubmitResult) -> AppResult<()> {
        if self.kind != StreamKind::Submit {
            return Ok(());
        }
        let status = proto::submit_result::Status::try_from(result.status)
            .unwrap_or(proto::submit_result::Status::Unspecified);
        match status {
            proto::submit_result::Status::Submitted => {
                if let Some(job_id) = result.job_id {
                    print_with_green_check_stdout(&format!("Job {job_id} submitted!"))
                        .map_err(|err| AppError::local_error(err.to_string()))?;
                    println!("Check job status with 'orbit job get {job_id}'");
                } else {
                    println!("Successfully submitted sbatch script.");
                }
                self.submit.job_id = result.job_id;
                self.submit.exit_code = Some(0);
            }
            proto::submit_result::Status::Failed => {
                let detail = result.detail.trim();
                if detail.is_empty() {
                    eprintln!("failed");
                } else {
                    eprintln!("failed: {}", format_server_error(detail));
                }
                if !detail.is_empty() {
                    self.submit.detail = Some(detail.to_string());
                }
                self.submit.exit_code = Some(1);
            }
            proto::submit_result::Status::Unspecified => {
                eprintln!("failed: submit result missing status");
                self.submit.detail = Some("submit result missing status".to_string());
                self.submit.exit_code = Some(1);
            }
        }
        Ok(())
    }

    fn take_stream_capture(&mut self) -> StreamCapture {
        std::mem::take(&mut self.stream)
    }

    fn take_submit_capture(&mut self) -> SubmitCapture {
        std::mem::take(&mut self.submit)
    }
}
