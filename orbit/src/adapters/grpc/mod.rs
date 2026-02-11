// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

mod submit_errors;

use std::future::Future;
use std::io::{IsTerminal, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::thread::JoinHandle;
use std::time::Duration as StdDuration;

use tokio::sync::mpsc;
use tokio::time::{Duration, timeout};
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Code, Request, Status};

use crate::app::commands::{AddClusterCapture, StreamCapture, SubmitCapture};
use crate::app::errors::{
    AppError, AppResult, ErrorType, describe_error_code, format_server_error,
};
use crate::app::ports::{InteractionPort, OrbitdPort, StreamOutputPort};
use proto::agent_client::AgentClient;
use proto::{
    AddClusterInit, AddClusterRequest, BuildProjectRequest, CancelJobRequest, CancelJobRequestInit,
    CleanupJobRequest, CleanupJobRequestInit, DeleteClusterRequest, DeleteProjectRequest,
    GetProjectRequest, JobLogsRequest, JobLogsRequestInit, ListClustersRequest, ListJobsRequest,
    ListProjectsRequest, LsRequest, LsRequestInit, ResolveHomeDirRequest,
    ResolveHomeDirRequestInit, RetrieveJobRequest, RetrieveJobRequestInit, SetClusterInit,
    SetClusterRequest, SubmitJobRequest, SubmitPathFilterRule, SubmitProjectRequest,
    UpsertProjectRequest, add_cluster_init, add_cluster_request, add_cluster_scratch_selection,
    resolve_home_dir_request, resolve_home_dir_request_init, set_cluster_request, stream_event,
    submit_job_request, submit_project_request, submit_stream_event,
};
use submit_errors::parse_remote_path_failure;

pub struct GrpcOrbitdPort {
    endpoint: String,
}

struct InlineSpinner {
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl InlineSpinner {
    fn start(message: &str) -> Option<Self> {
        if !std::io::stderr().is_terminal() {
            return None;
        }
        let stop = Arc::new(AtomicBool::new(false));
        let stop_signal = Arc::clone(&stop);
        let message = message.to_string();
        let handle = std::thread::spawn(move || {
            let frames = ['⠾', '⠷', '⠯', '⠟', '⠻', '⠽'];
            let mut idx = 0usize;
            while !stop_signal.load(Ordering::Relaxed) {
                let frame = frames[idx % frames.len()];
                let line = format!("\r{} {}", frame, message);
                let mut stderr = std::io::stderr();
                let _ = stderr.write_all(line.as_bytes());
                let _ = stderr.flush();
                std::thread::sleep(StdDuration::from_millis(120));
                idx = idx.wrapping_add(1);
            }
            let clear_width = message.len() + 2;
            let clear = format!("\r{}{}\r", " ".repeat(clear_width), " ");
            let mut stderr = std::io::stderr();
            let _ = stderr.write_all(clear.as_bytes());
            let _ = stderr.flush();
        });
        Some(Self {
            stop,
            handle: Some(handle),
        })
    }

    fn stop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for InlineSpinner {
    fn drop(&mut self) {
        self.stop();
    }
}

impl GrpcOrbitdPort {
    pub fn new(endpoint: String) -> Self {
        Self { endpoint }
    }

    async fn connect(&self) -> AppResult<AgentClient<tonic::transport::Channel>> {
        AgentClient::connect(self.endpoint.clone())
            .await
            .map_err(|_| AppError::daemon_unavailable(daemon_unavailable_message(&self.endpoint)))
    }
}

#[tonic::async_trait]
impl OrbitdPort for GrpcOrbitdPort {
    async fn ping(&self) -> AppResult<()> {
        let mut client = self.connect().await?;
        let ping_request = proto::PingRequest {
            message: "ping".into(),
        };
        let response = match timeout(Duration::from_secs(1), client.ping(ping_request)).await {
            Ok(res) => res.map_err(app_error_from_status)?,
            Err(elapsed) => {
                return Err(AppError::network_error(format!(
                    "Cancelled request after {elapsed} seconds"
                )));
            }
        };
        let message = response.get_ref().to_owned().message;
        match message.as_str() {
            "pong" => Ok(()),
            v => Err(AppError::remote_error(format!(
                "invalid response from server: expected 'pong', got '{v}'"
            ))),
        }
    }

    async fn list_clusters(
        &self,
        filter: &str,
        check_reachability: bool,
    ) -> AppResult<Vec<proto::ListClustersUnitResponse>> {
        let mut client = self.connect().await?;
        let list_clusters_request = ListClustersRequest {
            filter: filter.to_string(),
            check_reachability: Some(check_reachability),
        };
        let response = match timeout(
            Duration::from_secs(5),
            client.list_clusters(list_clusters_request),
        )
        .await
        {
            Ok(Ok(res)) => res.into_inner(),
            Ok(Err(status)) => return Err(app_error_from_status(status)),
            Err(e) => return Err(AppError::network_error(format!("operation timed out: {e}"))),
        };
        Ok(response.clusters)
    }

    async fn list_jobs(
        &self,
        cluster: Option<String>,
        project: Option<String>,
    ) -> AppResult<Vec<proto::ListJobsUnitResponse>> {
        let mut client = self.connect().await?;
        let list_jobs_request = ListJobsRequest {
            name: cluster,
            project_name: project,
        };
        let response =
            match timeout(Duration::from_secs(5), client.list_jobs(list_jobs_request)).await {
                Ok(Ok(res)) => res.into_inner(),
                Ok(Err(status)) => return Err(app_error_from_status(status)),
                Err(e) => return Err(AppError::network_error(format!("operation timed out: {e}"))),
            };
        Ok(response.jobs)
    }

    async fn upsert_project(&self, name: &str, path: &str) -> AppResult<proto::ProjectRecord> {
        let mut client = self.connect().await?;
        let request = UpsertProjectRequest {
            name: name.to_string(),
            path: path.to_string(),
        };
        let response = match timeout(Duration::from_secs(5), client.upsert_project(request)).await {
            Ok(Ok(res)) => res.into_inner(),
            Ok(Err(status)) => return Err(app_error_from_status(status)),
            Err(e) => return Err(AppError::network_error(format!("operation timed out: {e}"))),
        };
        response
            .project
            .ok_or_else(|| AppError::remote_error("missing project in response"))
    }

    async fn get_project(&self, name: &str) -> AppResult<proto::ProjectRecord> {
        let mut client = self.connect().await?;
        let request = GetProjectRequest {
            name: name.to_string(),
        };
        let response = match timeout(Duration::from_secs(5), client.get_project(request)).await {
            Ok(Ok(res)) => res.into_inner(),
            Ok(Err(status)) => return Err(app_error_from_status(status)),
            Err(e) => return Err(AppError::network_error(format!("operation timed out: {e}"))),
        };
        response
            .project
            .ok_or_else(|| AppError::remote_error("missing project in response"))
    }

    async fn list_projects(&self) -> AppResult<Vec<proto::ProjectRecord>> {
        let mut client = self.connect().await?;
        let request = ListProjectsRequest {};
        let response = match timeout(Duration::from_secs(5), client.list_projects(request)).await {
            Ok(Ok(res)) => res.into_inner(),
            Ok(Err(status)) => return Err(app_error_from_status(status)),
            Err(e) => return Err(AppError::network_error(format!("operation timed out: {e}"))),
        };
        Ok(response.projects)
    }

    async fn delete_project(&self, name: &str) -> AppResult<bool> {
        let mut client = self.connect().await?;
        let request = DeleteProjectRequest {
            name: name.to_string(),
        };
        let response = match timeout(Duration::from_secs(5), client.delete_project(request)).await {
            Ok(Ok(res)) => res.into_inner(),
            Ok(Err(status)) => return Err(app_error_from_status(status)),
            Err(e) => return Err(AppError::network_error(format!("operation timed out: {e}"))),
        };
        Ok(response.deleted)
    }

    async fn build_project(
        &self,
        path: String,
        package_git: bool,
    ) -> AppResult<proto::ProjectRecord> {
        let mut client = self.connect().await?;
        let request = BuildProjectRequest { path, package_git };
        let response = match timeout(Duration::from_secs(10), client.build_project(request)).await {
            Ok(Ok(res)) => res.into_inner(),
            Ok(Err(status)) => return Err(app_error_from_status(status)),
            Err(e) => return Err(AppError::network_error(format!("operation timed out: {e}"))),
        };
        response
            .project
            .ok_or_else(|| AppError::remote_error("missing project in response"))
    }

    async fn delete_cluster(&self, name: &str) -> AppResult<bool> {
        let mut client = self.connect().await?;
        let delete_request = DeleteClusterRequest {
            name: name.to_string(),
        };
        let response = match timeout(
            Duration::from_secs(5),
            client.delete_cluster(delete_request),
        )
        .await
        {
            Ok(Ok(res)) => res.into_inner(),
            Ok(Err(status)) => return Err(app_error_from_status(status)),
            Err(e) => return Err(AppError::network_error(format!("operation timed out: {e}"))),
        };
        Ok(response.deleted)
    }

    async fn ls(
        &self,
        name: String,
        job_id: Option<i64>,
        path: Option<String>,
        output: &mut dyn StreamOutputPort,
        interaction: &dyn InteractionPort,
    ) -> AppResult<StreamCapture> {
        let mut client = self.connect().await?;
        let (tx_ans, rx_ans) = mpsc::channel::<LsRequest>(16);
        let outbound = ReceiverStream::new(rx_ans);
        tx_ans
            .send(LsRequest {
                msg: Some(proto::ls_request::Msg::Init(LsRequestInit {
                    name,
                    path,
                    job_id,
                })),
            })
            .await
            .map_err(|_| AppError::internal_error("failed to send ls init"))?;

        let response = client
            .ls(Request::new(outbound))
            .await
            .map_err(app_error_from_status)?;
        let inbound = response.into_inner();
        handle_stream(
            inbound,
            output,
            interaction,
            move |answers| {
                let tx_ans = tx_ans.clone();
                async move {
                    tx_ans
                        .send(LsRequest {
                            msg: Some(proto::ls_request::Msg::Mfa(answers)),
                        })
                        .await
                        .map_err(|_| {
                            AppError::remote_error("server closed while sending MFA answers")
                        })
                }
            },
            |_| 1,
        )
        .await
    }

    async fn job_logs(
        &self,
        job_id: i64,
        stderr: bool,
        output: &mut dyn StreamOutputPort,
        interaction: &dyn InteractionPort,
    ) -> AppResult<StreamCapture> {
        let mut client = self.connect().await?;
        let (tx_ans, rx_ans) = mpsc::channel::<JobLogsRequest>(16);
        let outbound = ReceiverStream::new(rx_ans);
        tx_ans
            .send(JobLogsRequest {
                msg: Some(proto::job_logs_request::Msg::Init(JobLogsRequestInit {
                    job_id,
                    stderr,
                })),
            })
            .await
            .map_err(|_| AppError::internal_error("failed to send logs init"))?;

        let response = client
            .job_logs(Request::new(outbound))
            .await
            .map_err(app_error_from_status)?;
        let inbound = response.into_inner();
        handle_stream(
            inbound,
            output,
            interaction,
            move |answers| {
                let tx_ans = tx_ans.clone();
                async move {
                    tx_ans
                        .send(JobLogsRequest {
                            msg: Some(proto::job_logs_request::Msg::Mfa(answers)),
                        })
                        .await
                        .map_err(|_| {
                            AppError::remote_error("server closed while sending MFA answers")
                        })
                }
            },
            job_logs_error_exit_code,
        )
        .await
    }

    async fn job_cancel(
        &self,
        job_id: i64,
        output: &mut dyn StreamOutputPort,
        interaction: &dyn InteractionPort,
    ) -> AppResult<StreamCapture> {
        let mut client = self.connect().await?;
        let (tx_ans, rx_ans) = mpsc::channel::<CancelJobRequest>(16);
        let outbound = ReceiverStream::new(rx_ans);
        tx_ans
            .send(CancelJobRequest {
                msg: Some(proto::cancel_job_request::Msg::Init(CancelJobRequestInit {
                    job_id,
                })),
            })
            .await
            .map_err(|_| AppError::internal_error("failed to send cancel init"))?;

        let response = client
            .cancel_job(Request::new(outbound))
            .await
            .map_err(app_error_from_status)?;
        let inbound = response.into_inner();
        handle_stream(
            inbound,
            output,
            interaction,
            move |answers| {
                let tx_ans = tx_ans.clone();
                async move {
                    tx_ans
                        .send(CancelJobRequest {
                            msg: Some(proto::cancel_job_request::Msg::Mfa(answers)),
                        })
                        .await
                        .map_err(|_| {
                            AppError::remote_error("server closed while sending MFA answers")
                        })
                }
            },
            |_| 1,
        )
        .await
    }

    async fn job_cleanup(
        &self,
        job_id: i64,
        force: bool,
        full: bool,
        output: &mut dyn StreamOutputPort,
        interaction: &dyn InteractionPort,
    ) -> AppResult<StreamCapture> {
        let mut client = self.connect().await?;
        let (tx_ans, rx_ans) = mpsc::channel::<CleanupJobRequest>(16);
        let outbound = ReceiverStream::new(rx_ans);
        tx_ans
            .send(CleanupJobRequest {
                msg: Some(proto::cleanup_job_request::Msg::Init(
                    CleanupJobRequestInit {
                        job_id,
                        force,
                        full,
                    },
                )),
            })
            .await
            .map_err(|_| AppError::internal_error("failed to send cleanup init"))?;

        let response = client
            .cleanup_job(Request::new(outbound))
            .await
            .map_err(app_error_from_status)?;
        let inbound = response.into_inner();
        handle_stream(
            inbound,
            output,
            interaction,
            move |answers| {
                let tx_ans = tx_ans.clone();
                async move {
                    tx_ans
                        .send(CleanupJobRequest {
                            msg: Some(proto::cleanup_job_request::Msg::Mfa(answers)),
                        })
                        .await
                        .map_err(|_| {
                            AppError::remote_error("server closed while sending MFA answers")
                        })
                }
            },
            |_| 1,
        )
        .await
    }

    async fn job_retrieve(
        &self,
        job_id: i64,
        path: Option<String>,
        output: Option<PathBuf>,
        overwrite: bool,
        force: bool,
        output_port: &mut dyn StreamOutputPort,
        interaction: &dyn InteractionPort,
    ) -> AppResult<(PathBuf, StreamCapture)> {
        let mut client = self.connect().await?;
        let mut local_base = match output {
            Some(v) => v,
            None => {
                std::env::current_dir().map_err(|err| AppError::local_error(err.to_string()))?
            }
        };
        if !local_base.is_absolute() {
            local_base = std::env::current_dir()
                .map_err(|err| AppError::local_error(err.to_string()))?
                .join(local_base);
        }
        let local_path = local_base.to_string_lossy().into_owned();
        let local_target = match path.as_deref() {
            Some(requested) if !requested.trim().is_empty() => {
                resolve_retrieve_local_target(requested, &local_base)
            }
            _ => local_base.clone(),
        };
        let request_path = path.unwrap_or_default();

        let (tx_ans, rx_ans) = mpsc::channel::<RetrieveJobRequest>(16);
        let outbound = ReceiverStream::new(rx_ans);
        tx_ans
            .send(RetrieveJobRequest {
                msg: Some(proto::retrieve_job_request::Msg::Init(
                    RetrieveJobRequestInit {
                        job_id,
                        path: request_path,
                        local_path: Some(local_path),
                        overwrite,
                        force,
                    },
                )),
            })
            .await
            .map_err(|_| AppError::internal_error("failed to send retrieve init"))?;

        let response = client
            .retrieve_job(Request::new(outbound))
            .await
            .map_err(app_error_from_status)?;
        let inbound = response.into_inner();
        let capture = handle_stream(
            inbound,
            output_port,
            interaction,
            move |answers| {
                let tx_ans = tx_ans.clone();
                async move {
                    tx_ans
                        .send(RetrieveJobRequest {
                            msg: Some(proto::retrieve_job_request::Msg::Mfa(answers)),
                        })
                        .await
                        .map_err(|_| {
                            AppError::remote_error("server closed while sending MFA answers")
                        })
                }
            },
            job_retrieve_error_exit_code,
        )
        .await?;
        Ok((local_target, capture))
    }

    async fn submit_job(
        &self,
        name: String,
        local_path: String,
        remote_path: Option<String>,
        new_directory: bool,
        force: bool,
        sbatchscript: String,
        filters: Vec<SubmitPathFilterRule>,
        project_name: Option<String>,
        default_retrieve_path: Option<String>,
        template_values_json: Option<String>,
        output: &mut dyn StreamOutputPort,
        interaction: &dyn InteractionPort,
    ) -> AppResult<SubmitCapture> {
        let mut client = self.connect().await?;
        let (tx_ans, rx_ans) = mpsc::channel::<SubmitJobRequest>(16);
        let outbound = ReceiverStream::new(rx_ans);
        tx_ans
            .send(SubmitJobRequest {
                msg: Some(submit_job_request::Msg::Init(proto::SubmitJobRequestInit {
                    local_path,
                    remote_path,
                    name,
                    sbatchscript,
                    filters,
                    new_directory,
                    force,
                    project_name,
                    default_retrieve_path,
                    template_values_json,
                })),
            })
            .await
            .map_err(|_| AppError::internal_error("failed to send submit init"))?;

        let response = match client.submit_job(Request::new(outbound)).await {
            Ok(response) => response,
            Err(status) => {
                let message = format_status_error(&status);
                let failure = parse_remote_path_failure(status.message())
                    .map(|failure| (failure.remote_path.to_string(), failure.reason));
                let mut err = app_error_from_status(status);
                err.message = match failure {
                    Some((remote_path, reason)) => {
                        format!("{message}\nRemote path: {remote_path} - {reason}")
                    }
                    None => message,
                };
                return Err(err);
            }
        };
        let inbound = response.into_inner();
        handle_submit_stream(inbound, output, interaction, move |answers| {
            let tx_ans = tx_ans.clone();
            async move {
                tx_ans
                    .send(SubmitJobRequest {
                        msg: Some(submit_job_request::Msg::Mfa(answers)),
                    })
                    .await
                    .map_err(|_| AppError::remote_error("server closed while sending MFA answers"))
            }
        })
        .await
    }

    async fn submit_project(
        &self,
        project_name: String,
        project_tag: String,
        name: String,
        remote_path: Option<String>,
        new_directory: bool,
        force: bool,
        sbatchscript: String,
        filters: Vec<SubmitPathFilterRule>,
        default_retrieve_path: Option<String>,
        template_values_json: Option<String>,
        output: &mut dyn StreamOutputPort,
        interaction: &dyn InteractionPort,
    ) -> AppResult<SubmitCapture> {
        let mut client = self.connect().await?;
        let (tx_ans, rx_ans) = mpsc::channel::<SubmitProjectRequest>(16);
        let outbound = ReceiverStream::new(rx_ans);
        tx_ans
            .send(SubmitProjectRequest {
                msg: Some(submit_project_request::Msg::Init(
                    proto::SubmitProjectRequestInit {
                        project_name,
                        project_tag,
                        name,
                        sbatchscript,
                        filters,
                        new_directory,
                        force,
                        remote_path,
                        default_retrieve_path,
                        template_values_json,
                    },
                )),
            })
            .await
            .map_err(|_| AppError::internal_error("failed to send submit init"))?;

        let response = match client.submit_project(Request::new(outbound)).await {
            Ok(response) => response,
            Err(status) => {
                let message = format_status_error(&status);
                let failure = parse_remote_path_failure(status.message())
                    .map(|failure| (failure.remote_path.to_string(), failure.reason));
                let mut err = app_error_from_status(status);
                err.message = match failure {
                    Some((remote_path, reason)) => {
                        format!("{message}\nRemote path: {remote_path} - {reason}")
                    }
                    None => message,
                };
                return Err(err);
            }
        };
        let inbound = response.into_inner();
        handle_submit_stream(inbound, output, interaction, move |answers| {
            let tx_ans = tx_ans.clone();
            async move {
                tx_ans
                    .send(SubmitProjectRequest {
                        msg: Some(submit_project_request::Msg::Mfa(answers)),
                    })
                    .await
                    .map_err(|_| AppError::remote_error("server closed while sending MFA answers"))
            }
        })
        .await
    }

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
        output: &mut dyn StreamOutputPort,
        interaction: &dyn InteractionPort,
    ) -> AppResult<AddClusterCapture> {
        let mut client = self.connect().await?;
        let (tx_ans, rx_ans) = mpsc::channel::<AddClusterRequest>(16);
        let outbound = ReceiverStream::new(rx_ans);
        let host: add_cluster_init::Host = match hostname {
            Some(v) => add_cluster_init::Host::Hostname(v),
            None => match ip {
                Some(v) => add_cluster_init::Host::Ipaddr(v),
                None => {
                    return Err(AppError::invalid_argument(
                        "both hostname and ip address can't be none",
                    ));
                }
            },
        };
        let identity_path_expanded = match identity_path {
            Some(value) => Some(
                shellexpand::full(&value)
                    .map_err(|err| AppError::local_error(err.to_string()))?
                    .to_string(),
            ),
            None => None,
        };
        let init = AddClusterInit {
            name,
            username,
            host: Some(host),
            identity_path: identity_path_expanded,
            port,
            default_base_path,
            default_scratch_directory: default_scratch_directory.clone(),
            interactive_scratch_selection,
        };
        let acr = AddClusterRequest {
            msg: Some(add_cluster_request::Msg::Init(init)),
        };
        tx_ans
            .send(acr)
            .await
            .map_err(|_| AppError::internal_error("failed to send add cluster init"))?;
        let response = client
            .add_cluster(Request::new(outbound))
            .await
            .map_err(app_error_from_status)?;
        let mut inbound = response.into_inner();
        let mut selected_scratch_directory = default_scratch_directory;
        let mut scratch_options: Vec<String> = Vec::new();
        let spinner_message = "Gathering additional cluster information";
        let mut active_spinner_message: Option<String> = None;
        let mut pending_validation_path: Option<String> = None;
        let mut spinner = if interactive_scratch_selection && selected_scratch_directory.is_none() {
            active_spinner_message = Some(spinner_message.to_string());
            InlineSpinner::start(spinner_message)
        } else {
            None
        };
        while let Some(item) = inbound.next().await {
            match item {
                Ok(proto::StreamEvent { event: Some(ev) }) => match ev {
                    stream_event::Event::Stdout(bytes) => {
                        output.on_stdout(&bytes).await?;
                    }
                    stream_event::Event::Stderr(bytes) => {
                        output.on_stderr(&bytes).await?;
                    }
                    stream_event::Event::ExitCode(code) => {
                        if let Some(mut active_spinner) = spinner.take() {
                            active_spinner.stop();
                        }
                        output.on_exit_code(code).await?;
                        break;
                    }
                    stream_event::Event::Mfa(mfa) => {
                        if let Some(mut active_spinner) = spinner.take() {
                            active_spinner.stop();
                        }
                        let answers = interaction.prompt_mfa(&mfa).await?;
                        tx_ans
                            .send(AddClusterRequest {
                                msg: Some(proto::add_cluster_request::Msg::Mfa(answers)),
                            })
                            .await
                            .map_err(|_| {
                                AppError::remote_error("server closed while sending MFA answers")
                            })?;
                        if let Some(message) = active_spinner_message.as_deref() {
                            spinner = InlineSpinner::start(message);
                        }
                    }
                    stream_event::Event::Error(err) => {
                        if let Some(mut active_spinner) = spinner.take() {
                            active_spinner.stop();
                        }
                        output.on_error(&err).await?;
                        output.on_exit_code(1).await?;
                        break;
                    }
                    stream_event::Event::AddClusterScratchOptions(options) => {
                        if let Some(mut active_spinner) = spinner.take() {
                            active_spinner.stop();
                        }
                        scratch_options = options.directories;
                        let selection =
                            prompt_add_cluster_scratch_selection(interaction, &scratch_options)
                                .await?;
                        pending_validation_path = selection.clone();
                        if let Some(path) = selection.as_deref() {
                            let message = format!("Validating {path}...");
                            active_spinner_message = Some(message.clone());
                            spinner = InlineSpinner::start(&message);
                        } else {
                            active_spinner_message = None;
                        }
                        send_add_cluster_scratch_selection(&tx_ans, selection).await?;
                    }
                    stream_event::Event::AddClusterScratchValidation(validation) => {
                        if let Some(mut active_spinner) = spinner.take() {
                            active_spinner.stop();
                        }
                        active_spinner_message = None;
                        if validation.accepted {
                            selected_scratch_directory = validation.directory;
                            let scratch_label =
                                selected_scratch_directory.as_deref().unwrap_or("none");
                            let message = format!("✓ Scratch directory: {scratch_label}\n");
                            output.on_stderr(message.as_bytes()).await?;
                            pending_validation_path = None;
                        } else {
                            let failed_path = pending_validation_path
                                .as_deref()
                                .unwrap_or("<scratch directory>");
                            let failure_line = format!("✗ Validation of {failed_path} failed\n");
                            output.on_stderr(failure_line.as_bytes()).await?;
                            let selection =
                                prompt_add_cluster_scratch_selection(interaction, &scratch_options)
                                    .await?;
                            pending_validation_path = selection.clone();
                            if let Some(path) = selection.as_deref() {
                                let message = format!("Validating {path}...");
                                active_spinner_message = Some(message.clone());
                                spinner = InlineSpinner::start(&message);
                            } else {
                                active_spinner_message = None;
                            }
                            send_add_cluster_scratch_selection(&tx_ans, selection).await?;
                        }
                    }
                },
                Ok(proto::StreamEvent { event: None }) => {}
                Err(status) => {
                    if let Some(mut active_spinner) = spinner.take() {
                        active_spinner.stop();
                    }
                    let message = format_status_error(&status);
                    output.on_stderr(message.as_bytes()).await?;
                    output
                        .on_error(remote_code_for_status(status.code()))
                        .await?;
                    output.on_exit_code(1).await?;
                    break;
                }
            }
        }
        if let Some(mut active_spinner) = spinner.take() {
            active_spinner.stop();
        }

        Ok(AddClusterCapture {
            stream: output.take_stream_capture(),
            default_scratch_directory: selected_scratch_directory,
        })
    }

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
    ) -> AppResult<StreamCapture> {
        let mut client = self.connect().await?;
        let (tx_ans, rx_ans) = mpsc::channel::<SetClusterRequest>(16);
        let outbound = ReceiverStream::new(rx_ans);
        let identity_path_expanded = match identity_path {
            Some(value) => Some(
                shellexpand::full(&value)
                    .map_err(|err| AppError::local_error(err.to_string()))?
                    .to_string(),
            ),
            None => None,
        };
        let init = SetClusterInit {
            name,
            host,
            username,
            port,
            identity_path: identity_path_expanded,
            default_base_path,
        };
        let scr = SetClusterRequest {
            msg: Some(set_cluster_request::Msg::Init(init)),
        };
        tx_ans
            .send(scr)
            .await
            .map_err(|_| AppError::internal_error("failed to send set cluster init"))?;
        let response = client
            .set_cluster(Request::new(outbound))
            .await
            .map_err(app_error_from_status)?;
        let inbound = response.into_inner();
        handle_stream(
            inbound,
            output,
            interaction,
            move |answers| {
                let tx_ans = tx_ans.clone();
                async move {
                    tx_ans
                        .send(SetClusterRequest {
                            msg: Some(proto::set_cluster_request::Msg::Mfa(answers)),
                        })
                        .await
                        .map_err(|_| {
                            AppError::remote_error("server closed while sending MFA answers")
                        })
                }
            },
            |_| 1,
        )
        .await
    }

    async fn resolve_home_dir(
        &self,
        name: Option<String>,
        username: String,
        hostname: Option<String>,
        ip: Option<String>,
        identity_path: Option<String>,
        port: u32,
        interaction: &dyn InteractionPort,
    ) -> AppResult<String> {
        let mut client = self.connect().await?;
        let (tx_ans, rx_ans) = mpsc::channel::<ResolveHomeDirRequest>(16);
        let outbound = ReceiverStream::new(rx_ans);
        let host: resolve_home_dir_request_init::Host = match hostname {
            Some(v) => resolve_home_dir_request_init::Host::Hostname(v),
            None => match ip {
                Some(v) => resolve_home_dir_request_init::Host::Ipaddr(v),
                None => {
                    return Err(AppError::invalid_argument(
                        "both hostname and ip address can't be none",
                    ));
                }
            },
        };
        let identity_path_expanded = match identity_path {
            Some(value) => Some(
                shellexpand::full(&value)
                    .map_err(|err| AppError::local_error(err.to_string()))?
                    .to_string(),
            ),
            None => None,
        };
        let init = ResolveHomeDirRequestInit {
            username,
            host: Some(host),
            identity_path: identity_path_expanded,
            port,
            name,
        };
        let req = ResolveHomeDirRequest {
            msg: Some(resolve_home_dir_request::Msg::Init(init)),
        };
        tx_ans
            .send(req)
            .await
            .map_err(|_| AppError::internal_error("failed to send resolve home init"))?;
        let response = client
            .resolve_home_dir(Request::new(outbound))
            .await
            .map_err(app_error_from_status)?;
        let mut inbound = response.into_inner();
        let tx_mfa = tx_ans.clone();
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let mut exit_code = None;
        let mut mfa_lines = 0usize;
        let mut saw_mfa = false;
        while let Some(item) = inbound.next().await {
            match item {
                Ok(event) => match event.event {
                    Some(stream_event::Event::Stdout(bytes)) => {
                        stdout.extend_from_slice(&bytes);
                    }
                    Some(stream_event::Event::Stderr(bytes)) => {
                        stderr.extend_from_slice(&bytes);
                    }
                    Some(stream_event::Event::ExitCode(code)) => {
                        exit_code = Some(code);
                        break;
                    }
                    Some(stream_event::Event::Mfa(mfa)) => {
                        saw_mfa = true;
                        let (answers, lines) = interaction.prompt_mfa_transient(&mfa).await?;
                        mfa_lines = mfa_lines.saturating_add(lines);
                        tx_mfa
                            .send(ResolveHomeDirRequest {
                                msg: Some(resolve_home_dir_request::Msg::Mfa(answers)),
                            })
                            .await
                            .map_err(|_| {
                                AppError::remote_error("server closed while sending MFA answers")
                            })?;
                    }
                    Some(stream_event::Event::Error(err)) => {
                        return Err(AppError::remote_error(format_server_error(&err)));
                    }
                    Some(_) => {}
                    None => {}
                },
                Err(status) => {
                    return Err(app_error_from_status(status));
                }
            }
        }

        if exit_code != Some(0) {
            let detail = if stderr.is_empty() {
                "unknown error".to_string()
            } else {
                String::from_utf8_lossy(&stderr).to_string()
            };
            return Err(AppError::remote_error(format!(
                "failed to resolve remote home directory: {}",
                format_server_error(detail.trim())
            )));
        }

        if saw_mfa && mfa_lines > 0 {
            interaction.clear_transient(mfa_lines).await?;
        }

        let home_raw = String::from_utf8(stdout)
            .map_err(|e| AppError::local_error(format!("invalid UTF-8: {e}")))?;
        let home = home_raw.trim();
        if home.is_empty() {
            return Err(AppError::remote_error("remote home directory is empty"));
        }
        Ok(home.to_string())
    }
}

async fn prompt_add_cluster_scratch_selection(
    interaction: &dyn InteractionPort,
    discovered: &[String],
) -> AppResult<Option<String>> {
    let mut options = discovered.to_vec();
    options.push("other".to_string());
    options.push("none".to_string());
    let default = discovered.first().map(String::as_str).or(Some("none"));
    let help = "Choose a scratch directory, enter a custom path, or select none.";

    loop {
        let selected = interaction
            .select_enum("scratch directory", &options, default, help)
            .await?;
        if selected == "none" {
            return Ok(None);
        }
        if selected == "other" {
            let value = interaction
                .prompt_line(
                    "Scratch directory: ",
                    "Remote scratch directory path (leave empty to keep none).",
                )
                .await?;
            let trimmed = value.trim();
            if trimmed.is_empty() {
                return Ok(None);
            }
            return Ok(Some(trimmed.to_string()));
        }
        return Ok(Some(selected));
    }
}

async fn send_add_cluster_scratch_selection(
    sender: &mpsc::Sender<AddClusterRequest>,
    selection: Option<String>,
) -> AppResult<()> {
    let selection = match selection {
        Some(path) => Some(add_cluster_scratch_selection::Selection::Directory(path)),
        None => Some(add_cluster_scratch_selection::Selection::None(true)),
    };
    sender
        .send(AddClusterRequest {
            msg: Some(add_cluster_request::Msg::ScratchSelection(
                proto::AddClusterScratchSelection { selection },
            )),
        })
        .await
        .map_err(|_| AppError::remote_error("server closed while sending scratch selection"))
}

fn daemon_unavailable_message(daemon_endpoint: &str) -> String {
    format!("Could not contact the orbitd server at {daemon_endpoint}. Is it running?")
}

fn format_status_error(status: &Status) -> String {
    let message = status.message();
    if let Some(message) = describe_error_code(message) {
        return message.to_string();
    }
    if !message.is_empty() {
        return message.to_string();
    }
    match status.code() {
        Code::Cancelled => describe_error_code("canceled")
            .unwrap_or("Canceled.")
            .to_string(),
        Code::Unauthenticated => describe_error_code("authentication_failure")
            .unwrap_or("Authentication failed.")
            .to_string(),
        Code::PermissionDenied => describe_error_code("permission_denied")
            .unwrap_or("Permission denied.")
            .to_string(),
        Code::Unavailable => describe_error_code("network_error")
            .unwrap_or("Network error.")
            .to_string(),
        _ => "Server error.".to_string(),
    }
}

fn app_error_from_status(status: Status) -> AppError {
    let message = format_status_error(&status);
    let kind = match status.code() {
        Code::InvalidArgument => ErrorType::InvalidArgument,
        Code::PermissionDenied | Code::Unauthenticated => ErrorType::PermissionDenied,
        Code::AlreadyExists => ErrorType::Conflict,
        Code::Unavailable => ErrorType::NetworkError,
        _ => ErrorType::RemoteError,
    };
    AppError::new(kind, message)
}

async fn handle_stream<S, F, Fut>(
    mut inbound: S,
    output: &mut dyn StreamOutputPort,
    interaction: &dyn InteractionPort,
    mut send_mfa: F,
    map_error_code: fn(&str) -> i32,
) -> AppResult<StreamCapture>
where
    S: tokio_stream::Stream<Item = Result<proto::StreamEvent, Status>> + Unpin,
    F: FnMut(proto::MfaAnswer) -> Fut,
    Fut: Future<Output = AppResult<()>>,
{
    while let Some(item) = inbound.next().await {
        match item {
            Ok(proto::StreamEvent { event: Some(ev) }) => match ev {
                stream_event::Event::Stdout(bytes) => {
                    output.on_stdout(&bytes).await?;
                }
                stream_event::Event::Stderr(bytes) => {
                    output.on_stderr(&bytes).await?;
                }
                stream_event::Event::ExitCode(code) => {
                    output.on_exit_code(code).await?;
                    break;
                }
                stream_event::Event::Mfa(mfa) => {
                    let answers = interaction.prompt_mfa(&mfa).await?;
                    send_mfa(answers).await?;
                }
                stream_event::Event::Error(err) => {
                    output.on_error(&err).await?;
                    output.on_exit_code(map_error_code(&err)).await?;
                    break;
                }
                _ => {}
            },
            Ok(proto::StreamEvent { event: None }) => {}
            Err(status) => {
                let message = format_status_error(&status);
                let remote_code = remote_code_for_status(status.code());
                output.on_stderr(message.as_bytes()).await?;
                output.on_error(remote_code).await?;
                output.on_exit_code(map_error_code(remote_code)).await?;
                break;
            }
        }
    }

    Ok(output.take_stream_capture())
}

async fn handle_submit_stream<S, F, Fut>(
    mut inbound: S,
    output: &mut dyn StreamOutputPort,
    interaction: &dyn InteractionPort,
    mut send_mfa: F,
) -> AppResult<SubmitCapture>
where
    S: tokio_stream::Stream<Item = Result<proto::SubmitStreamEvent, Status>> + Unpin,
    F: FnMut(proto::MfaAnswer) -> Fut,
    Fut: Future<Output = AppResult<()>>,
{
    while let Some(item) = inbound.next().await {
        match item {
            Ok(proto::SubmitStreamEvent { event: Some(ev) }) => match ev {
                submit_stream_event::Event::Stdout(bytes) => {
                    output.on_stdout(&bytes).await?;
                }
                submit_stream_event::Event::Stderr(bytes) => {
                    output.on_stderr(&bytes).await?;
                }
                submit_stream_event::Event::ExitCode(code) => {
                    output.on_exit_code(code).await?;
                    break;
                }
                submit_stream_event::Event::Mfa(mfa) => {
                    let answers = interaction.prompt_mfa(&mfa).await?;
                    send_mfa(answers).await?;
                }
                submit_stream_event::Event::Error(err) => {
                    output.on_error(&err).await?;
                    output.on_exit_code(1).await?;
                    break;
                }
                submit_stream_event::Event::SubmitStatus(status) => {
                    output.on_submit_status(&status).await?;
                }
                submit_stream_event::Event::SubmitResult(result) => {
                    output.on_submit_result(&result).await?;
                    break;
                }
            },
            Ok(proto::SubmitStreamEvent { event: None }) => {}
            Err(status) => {
                let message = format_status_error(&status);
                output.on_stderr(message.as_bytes()).await?;
                output
                    .on_error(remote_code_for_status(status.code()))
                    .await?;
                output.on_exit_code(1).await?;
                break;
            }
        }
    }

    Ok(output.take_submit_capture())
}

fn job_logs_error_exit_code(err: &str) -> i32 {
    match err {
        "invalid_argument" => 2,
        "not_found" => 3,
        _ => 1,
    }
}

fn job_retrieve_error_exit_code(err: &str) -> i32 {
    match err {
        "invalid_argument" => 2,
        "not_found" => 3,
        "conflict" => 4,
        _ => 1,
    }
}

fn remote_code_for_status(code: Code) -> &'static str {
    match code {
        Code::InvalidArgument => "invalid_argument",
        Code::NotFound => "not_found",
        Code::AlreadyExists => "conflict",
        Code::PermissionDenied | Code::Unauthenticated => "permission_denied",
        _ => "remote_error",
    }
}

fn normalize_path(p: impl AsRef<Path>) -> PathBuf {
    let mut out = PathBuf::new();
    let p = p.as_ref();
    let mut comps = p.components().peekable();
    while let Some(c) = comps.peek() {
        match c {
            Component::Prefix(prefix) => {
                out.push(Path::new(prefix.as_os_str()));
                comps.next();
            }
            Component::RootDir => {
                out.push(Path::new(std::path::MAIN_SEPARATOR_STR));
                comps.next();
            }
            _ => break,
        }
    }

    for comp in comps {
        match comp {
            Component::CurDir => {}
            Component::ParentDir => {
                let popped = out.pop();
                if !popped || out.as_os_str().is_empty() {
                    out.push("..");
                }
            }
            Component::Normal(seg) => {
                out.push(seg);
            }
            Component::Prefix(_) | Component::RootDir => {}
        }
    }

    out
}

fn resolve_retrieve_local_target(path: &str, local_base: &Path) -> PathBuf {
    if Path::new(path).is_absolute() {
        let normalized = normalize_path(Path::new(path));
        match normalized.file_name() {
            Some(name) => local_base.join(name),
            None => local_base.to_path_buf(),
        }
    } else {
        match Path::new(path).file_name() {
            Some(name) => local_base.join(name),
            None => local_base.to_path_buf(),
        }
    }
}
