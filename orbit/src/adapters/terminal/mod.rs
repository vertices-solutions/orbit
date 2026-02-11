// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

mod console;
mod enum_picker;
mod format;
mod mfa;
mod prompt;
mod sbatch_picker;

use std::io::Write;

use crate::app::commands::{
    CommandResult, InitActionStatus, ProjectInitAction, StreamCapture, SubmitCapture,
};
use crate::app::errors::{AppError, AppResult, format_server_error};
use crate::app::ports::{
    InteractionPort, OutputPort, PromptFeedbackPort, PromptLine, StreamKind, StreamOutputPort,
};
use console::{
    Spinner, SpinnerTarget, print_with_green_check_stderr, print_with_green_check_stdout,
    print_with_red_cross_stderr,
};
use enum_picker::pick_enum_value;
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

fn render_project_init_actions(actions: &[ProjectInitAction]) -> AppResult<()> {
    for action in actions {
        match &action.status {
            InitActionStatus::Success => {
                print_with_green_check_stdout(&action.message)
                    .map_err(|err| AppError::local_error(err.to_string()))?;
            }
            InitActionStatus::Failed(reason) => {
                let message = if reason.is_empty() {
                    action.message.clone()
                } else {
                    format!("{}: {}", action.message, reason)
                };
                eprintln!("{message}");
            }
        }
    }
    Ok(())
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
                if jobs.is_empty() {
                    println!("No jobs registered");
                } else {
                    print!("{}", format_jobs_table(jobs));
                }
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
            CommandResult::ClusterList {
                clusters,
                check_reachability,
            } => {
                print!("{}", format_clusters_table(clusters, *check_reachability));
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
            CommandResult::ProjectInit { name, actions, .. } => {
                render_project_init_actions(actions)?;
                println!("Project '{}' initialized", name);
            }
            CommandResult::ProjectBuild { project } => {
                let name = if let Some(tag) = project.version_tag.as_deref() {
                    format!("{}:{}", project.name, tag)
                } else {
                    project.name.clone()
                };
                print_with_green_check_stdout(&format!("built {name} successfully"))
                    .map_err(|err| AppError::local_error(err.to_string()))?;
            }
            CommandResult::ProjectList { projects } => {
                if projects.is_empty() {
                    println!("No projects registered.");
                } else {
                    print!("{}", format_projects_table(projects));
                }
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

struct TerminalPromptFeedback {
    inner: prompt::PromptFeedback,
}

impl PromptFeedbackPort for TerminalPromptFeedback {
    fn start_information_gathering(&mut self, message: &str) -> AppResult<()> {
        self.inner
            .start_information_gathering(message)
            .map_err(|err| AppError::local_error(err.to_string()))
    }

    fn stop_information_gathering(&mut self) -> AppResult<()> {
        self.inner.stop_information_gathering();
        Ok(())
    }

    fn start_validation(&mut self, message: &str) -> AppResult<()> {
        self.inner
            .start_validation(message)
            .map_err(|err| AppError::local_error(err.to_string()))
    }

    fn stop_validation(&mut self) -> AppResult<()> {
        self.inner
            .stop_validation()
            .map_err(|err| AppError::local_error(err.to_string()))
    }

    fn finish_success(&mut self, message: &str) -> AppResult<()> {
        self.inner
            .finish_success(message)
            .map_err(|err| AppError::local_error(err.to_string()))
    }

    fn finish_failure(&mut self, message: &str) -> AppResult<()> {
        self.inner
            .finish_failure(message)
            .map_err(|err| AppError::local_error(err.to_string()))
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

    async fn prompt_line_confirmable(&self, prompt: &str, help: &str) -> AppResult<PromptLine> {
        prompt::prompt_line_confirmable(prompt, help)
            .map(|(input, feedback)| {
                PromptLine::new(
                    input,
                    Some(Box::new(TerminalPromptFeedback { inner: feedback })),
                )
            })
            .map_err(|err| AppError::local_error(err.to_string()))
    }

    async fn prompt_line_with_default_confirmable(
        &self,
        prompt: &str,
        help: &str,
        default: &str,
    ) -> AppResult<PromptLine> {
        prompt::prompt_line_with_default_confirmable(prompt, help, Some(default))
            .map(|(input, feedback)| {
                PromptLine::new(
                    input,
                    Some(Box::new(TerminalPromptFeedback { inner: feedback })),
                )
            })
            .map_err(|err| AppError::local_error(err.to_string()))
    }

    async fn prompt_feedback(&self) -> AppResult<Box<dyn PromptFeedbackPort>> {
        Ok(Box::new(TerminalPromptFeedback {
            inner: prompt::PromptFeedback::new(),
        }))
    }

    async fn select_sbatch(&self, options: &[String]) -> AppResult<Option<String>> {
        if options.is_empty() {
            return Ok(None);
        }
        pick_sbatch_script(options.to_vec())
            .map(Some)
            .map_err(|err| AppError::local_error(err.to_string()))
    }

    async fn select_enum(
        &self,
        name: &str,
        options: &[String],
        default: Option<&str>,
        help: &str,
    ) -> AppResult<String> {
        pick_enum_value(name, help, options.to_vec(), default)
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
    transfer_spinner: Option<Spinner>,
}

impl TerminalStreamOutput {
    fn new(kind: StreamKind) -> Self {
        Self {
            kind,
            stream: StreamCapture::default(),
            submit: SubmitCapture::default(),
            printed_remote_path: false,
            transfer_spinner: None,
        }
    }

    fn record_submit(&mut self, f: impl FnOnce(&mut SubmitCapture)) {
        if self.kind == StreamKind::Submit {
            f(&mut self.submit);
        }
    }

    fn start_transfer_spinner(&mut self) {
        if self.transfer_spinner.is_some() {
            return;
        }
        self.transfer_spinner = Spinner::start("Transferring files...", SpinnerTarget::Stderr);
    }

    fn stop_transfer_spinner(&mut self) {
        if let Some(mut spinner) = self.transfer_spinner.take() {
            spinner.stop();
        }
    }
}

fn is_submit_conflict_message(text: &str) -> bool {
    let trimmed = text.trim();
    if !trimmed.starts_with("job ") {
        return false;
    }
    let infix = " is still running in ";
    let suffix = "; use --force to submit anyway";
    let Some((_, rest)) = trimmed.split_once(infix) else {
        return false;
    };
    let Some((path, _)) = rest.split_once(suffix) else {
        return false;
    };
    !path.trim().is_empty()
}

enum StderrStatusLine<'a> {
    GreenCheck(&'a str),
    RedCross(&'a str),
}

fn parse_stderr_status_line(bytes: &[u8]) -> Option<StderrStatusLine<'_>> {
    let text = std::str::from_utf8(bytes).ok()?;
    let line = text.strip_suffix('\n')?;
    if line.contains('\n') {
        return None;
    }
    if let Some(message) = line.strip_prefix("✓ ") {
        return Some(StderrStatusLine::GreenCheck(message));
    }
    if let Some(message) = line.strip_prefix("✗ ") {
        return Some(StderrStatusLine::RedCross(message));
    }
    None
}

#[tonic::async_trait]
impl StreamOutputPort for TerminalStreamOutput {
    async fn on_stdout(&mut self, bytes: &[u8]) -> AppResult<()> {
        if self.kind == StreamKind::Submit {
            self.stop_transfer_spinner();
        }
        std::io::stdout()
            .write_all(bytes)
            .and_then(|_| std::io::stdout().flush())
            .map_err(|err| AppError::local_error(err.to_string()))?;
        self.stream.stdout.extend_from_slice(bytes);
        self.record_submit(|capture| capture.stdout.extend_from_slice(bytes));
        Ok(())
    }

    async fn on_stderr(&mut self, bytes: &[u8]) -> AppResult<()> {
        if self.kind == StreamKind::Submit {
            self.stop_transfer_spinner();
        }
        let mut printed = true;
        if self.kind == StreamKind::Submit {
            if let Ok(text) = std::str::from_utf8(bytes) {
                if is_submit_conflict_message(text) {
                    printed = false;
                }
            }
        }
        if printed {
            match parse_stderr_status_line(bytes) {
                Some(StderrStatusLine::GreenCheck(message)) => {
                    print_with_green_check_stderr(message)
                        .map_err(|err| AppError::local_error(err.to_string()))?;
                }
                Some(StderrStatusLine::RedCross(message)) => {
                    print_with_red_cross_stderr(message)
                        .map_err(|err| AppError::local_error(err.to_string()))?;
                }
                None => {
                    std::io::stderr()
                        .write_all(bytes)
                        .and_then(|_| std::io::stderr().flush())
                        .map_err(|err| AppError::local_error(err.to_string()))?;
                }
            }
        }
        self.stream.stderr.extend_from_slice(bytes);
        self.record_submit(|capture| capture.stderr.extend_from_slice(bytes));
        Ok(())
    }

    async fn on_exit_code(&mut self, code: i32) -> AppResult<()> {
        if self.kind == StreamKind::Submit {
            self.stop_transfer_spinner();
        }
        self.stream.exit_code = Some(code);
        self.record_submit(|capture| capture.exit_code = Some(code));
        Ok(())
    }

    async fn on_error(&mut self, code: &str) -> AppResult<()> {
        if self.kind == StreamKind::Submit {
            self.stop_transfer_spinner();
        }
        self.stream.error_code = Some(code.to_string());
        self.record_submit(|capture| capture.error_code = Some(code.to_string()));
        if self.kind != StreamKind::Submit {
            eprintln!("{}", format_server_error(code));
        }
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
            proto::submit_status::Phase::TransferStart => {
                self.start_transfer_spinner();
            }
            proto::submit_status::Phase::TransferDone => {
                self.stop_transfer_spinner();
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
        self.stop_transfer_spinner();
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
