// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

use serde_json::{Value, json};

use crate::app::commands::{CommandResult, StreamCapture, SubmitCapture};
use crate::app::errors::{AppError, AppResult};
use crate::app::ports::{InteractionPort, OutputPort, StreamKind, StreamOutputPort};
use crate::format::{cluster_to_json, job_to_json};

pub struct JsonOutput;

impl JsonOutput {
    pub fn new() -> Self {
        Self
    }
}

#[tonic::async_trait]
impl OutputPort for JsonOutput {
    async fn render(&self, result: &CommandResult) -> AppResult<()> {
        let payload = json!({
            "ok": true,
            "result": result_to_json(result),
        });
        let output = serde_json::to_string_pretty(&payload)
            .map_err(|err| AppError::internal_error(err.to_string()))?;
        println!("{output}");
        Ok(())
    }

    async fn render_error(&self, error: &AppError) -> AppResult<()> {
        let payload = json!({
            "ok": false,
            "errorType": error.kind.as_str(),
            "reason": error.message,
        });
        let output = serde_json::to_string_pretty(&payload)
            .map_err(|err| AppError::internal_error(err.to_string()))?;
        eprintln!("{output}");
        Ok(())
    }

    async fn info(&self, _message: &str) -> AppResult<()> {
        Ok(())
    }

    async fn warn(&self, _message: &str) -> AppResult<()> {
        Ok(())
    }

    async fn success(&self, _message: &str) -> AppResult<()> {
        Ok(())
    }

    fn stream_output(&self, kind: StreamKind) -> Box<dyn StreamOutputPort> {
        Box::new(JsonStreamOutput::new(kind))
    }
}

pub struct NonInteractiveInteraction;

impl NonInteractiveInteraction {
    pub fn new() -> Self {
        Self
    }
}

#[tonic::async_trait]
impl InteractionPort for NonInteractiveInteraction {
    async fn confirm(&self, _prompt: &str, _help: &str) -> AppResult<bool> {
        Err(AppError::confirmation_required(
            "confirmation required; pass --yes to proceed in non-interactive mode",
        ))
    }

    async fn prompt_line(&self, _prompt: &str, _help: &str) -> AppResult<String> {
        Err(AppError::invalid_argument(
            "input required; rerun without --non-interactive",
        ))
    }

    async fn prompt_line_with_default(
        &self,
        _prompt: &str,
        _help: &str,
        _default: &str,
    ) -> AppResult<String> {
        Err(AppError::invalid_argument(
            "input required; rerun without --non-interactive",
        ))
    }

    async fn select_sbatch(&self, _options: &[String]) -> AppResult<Option<String>> {
        Err(AppError::invalid_argument(
            "interactive selection required; rerun without --non-interactive",
        ))
    }

    async fn prompt_mfa(&self, _mfa: &proto::MfaPrompt) -> AppResult<proto::MfaAnswer> {
        Err(AppError::mfa_required(
            "MFA required; rerun without --non-interactive",
        ))
    }

    async fn prompt_mfa_transient(
        &self,
        _mfa: &proto::MfaPrompt,
    ) -> AppResult<(proto::MfaAnswer, usize)> {
        Err(AppError::mfa_required(
            "MFA required; rerun without --non-interactive",
        ))
    }

    async fn clear_transient(&self, _lines: usize) -> AppResult<()> {
        Ok(())
    }
}

struct JsonStreamOutput {
    kind: StreamKind,
    stream: StreamCapture,
    submit: SubmitCapture,
}

impl JsonStreamOutput {
    fn new(kind: StreamKind) -> Self {
        Self {
            kind,
            stream: StreamCapture::default(),
            submit: SubmitCapture::default(),
        }
    }
}

#[tonic::async_trait]
impl StreamOutputPort for JsonStreamOutput {
    async fn on_stdout(&mut self, bytes: &[u8]) -> AppResult<()> {
        self.stream.stdout.extend_from_slice(bytes);
        if self.kind == StreamKind::Submit {
            self.submit.stdout.extend_from_slice(bytes);
        }
        Ok(())
    }

    async fn on_stderr(&mut self, bytes: &[u8]) -> AppResult<()> {
        self.stream.stderr.extend_from_slice(bytes);
        if self.kind == StreamKind::Submit {
            self.submit.stderr.extend_from_slice(bytes);
        }
        Ok(())
    }

    async fn on_exit_code(&mut self, code: i32) -> AppResult<()> {
        self.stream.exit_code = Some(code);
        if self.kind == StreamKind::Submit {
            self.submit.exit_code = Some(code);
        }
        Ok(())
    }

    async fn on_error(&mut self, code: &str) -> AppResult<()> {
        self.stream.error_code = Some(code.to_string());
        if self.kind == StreamKind::Submit {
            self.submit.error_code = Some(code.to_string());
        }
        Ok(())
    }

    async fn on_submit_status(&mut self, status: &proto::SubmitStatus) -> AppResult<()> {
        if self.kind == StreamKind::Submit {
            let phase = proto::submit_status::Phase::try_from(status.phase)
                .unwrap_or(proto::submit_status::Phase::Unspecified);
            if phase == proto::submit_status::Phase::Resolved && !status.remote_path.is_empty() {
                self.submit.remote_path = Some(status.remote_path.clone());
            }
        }
        Ok(())
    }

    async fn on_submit_result(&mut self, result: &proto::SubmitResult) -> AppResult<()> {
        if self.kind == StreamKind::Submit {
            let status = proto::submit_result::Status::try_from(result.status)
                .unwrap_or(proto::submit_result::Status::Unspecified);
            match status {
                proto::submit_result::Status::Submitted => {
                    self.submit.job_id = result.job_id;
                    self.submit.exit_code = Some(0);
                }
                proto::submit_result::Status::Failed => {
                    let detail = result.detail.trim();
                    if !detail.is_empty() {
                        self.submit.detail = Some(detail.to_string());
                    }
                    self.submit.exit_code = Some(1);
                }
                proto::submit_result::Status::Unspecified => {
                    self.submit.detail = Some("submit result missing status".to_string());
                    self.submit.exit_code = Some(1);
                }
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

fn bytes_to_string(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).to_string()
}

fn stream_capture_json(capture: &StreamCapture) -> Value {
    json!({
        "stdout": bytes_to_string(&capture.stdout),
        "stderr": bytes_to_string(&capture.stderr),
        "exit_code": capture.exit_code.unwrap_or(0),
    })
}

fn submit_capture_json(
    capture: &SubmitCapture,
    cluster: &str,
    local_path: &str,
    sbatchscript: &str,
) -> Value {
    json!({
        "job_id": capture.job_id,
        "remote_path": capture.remote_path,
        "local_path": local_path,
        "cluster": cluster,
        "sbatchscript": sbatchscript,
        "status": "submitted",
        "stdout": bytes_to_string(&capture.stdout),
        "stderr": bytes_to_string(&capture.stderr),
    })
}

fn result_to_json(result: &CommandResult) -> Value {
    match result {
        CommandResult::Message { message } => json!({ "message": message }),
        CommandResult::Pong { message } => json!({ "message": message }),
        CommandResult::JobList { jobs } => {
            let data: Vec<Value> = jobs.iter().map(job_to_json).collect();
            Value::Array(data)
        }
        CommandResult::JobDetails { job } => job_to_json(job),
        CommandResult::JobSubmit {
            cluster,
            local_path,
            sbatchscript,
            capture,
        } => submit_capture_json(capture, cluster, local_path, sbatchscript),
        CommandResult::JobLogs { capture } => stream_capture_json(capture),
        CommandResult::JobCancel { job_id, capture } => json!({
            "job_id": job_id,
            "status": "canceled",
            "stdout": bytes_to_string(&capture.stdout),
            "stderr": bytes_to_string(&capture.stderr),
        }),
        CommandResult::JobCleanup {
            job_id,
            force,
            full,
            capture,
        } => json!({
            "job_id": job_id,
            "status": "cleaned",
            "force": force,
            "full": full,
            "stdout": bytes_to_string(&capture.stdout),
            "stderr": bytes_to_string(&capture.stderr),
        }),
        CommandResult::JobLs { capture } => stream_capture_json(capture),
        CommandResult::JobRetrieve {
            job_id,
            path,
            output,
            capture,
        } => json!({
            "job_id": job_id,
            "path": path,
            "output": output.display().to_string(),
            "stdout": bytes_to_string(&capture.stdout),
            "stderr": bytes_to_string(&capture.stderr),
            "exit_code": capture.exit_code.unwrap_or(0),
        }),
        CommandResult::ClusterList { clusters } => {
            let data: Vec<Value> = clusters.iter().map(cluster_to_json).collect();
            Value::Array(data)
        }
        CommandResult::ClusterDetails { cluster } => cluster_to_json(cluster),
        CommandResult::ClusterLs { capture } => stream_capture_json(capture),
        CommandResult::ClusterAdd {
            name,
            username,
            hostname,
            ip,
            port,
            identity_path,
            default_base_path,
        } => json!({
            "name": name,
            "username": username,
            "hostname": hostname,
            "ip": ip,
            "port": port,
            "identity_path": identity_path,
            "default_base_path": default_base_path,
            "status": "added",
        }),
        CommandResult::ClusterSet { name, .. } => json!({
            "name": name,
            "status": "updated",
        }),
        CommandResult::ClusterDelete { name } => json!({
            "name": name,
            "status": "deleted",
        }),
    }
}
