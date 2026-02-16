// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

mod run_errors;

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

use crate::app::commands::{AddClusterCapture, RunCapture, StreamCapture};
use crate::app::errors::{
    AppError, AppResult, ErrorType, describe_error_code, format_server_error,
};
use crate::app::ports::{
    InteractionPort, OrbitdPort, PromptFeedbackPort, PromptLine, StreamOutputPort,
};
use proto::agent_client::AgentClient;
use proto::{
    AddClusterInit, AddClusterRequest, BuildBlueprintRequest, CancelJobRequest,
    CancelJobRequestInit, CleanupJobRequest, CleanupJobRequestInit, DeleteBlueprintRequest,
    DeleteClusterRequest, GetBlueprintRequest, JobLogsRequest, JobLogsRequestInit,
    ListAccountsRequest, ListBlueprintsRequest, ListClustersRequest, ListJobsRequest,
    ListPartitionsRequest, LsRequest, LsRequestInit, ResolveHomeDirRequest,
    ResolveHomeDirRequestInit, RetrieveJobRequest, RetrieveJobRequestInit, RunBlueprintRequest,
    RunJobRequest, RunPathFilterRule, SetClusterInit, SetClusterRequest, UpsertBlueprintRequest,
    add_cluster_init, add_cluster_request, add_cluster_scratch_selection, resolve_home_dir_request,
    resolve_home_dir_request_init, run_blueprint_request, run_job_request, run_stream_event,
    set_cluster_request, stream_event,
};
use run_errors::parse_remote_path_failure;

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
        blueprint: Option<String>,
    ) -> AppResult<Vec<proto::ListJobsUnitResponse>> {
        let mut client = self.connect().await?;
        let list_jobs_request = ListJobsRequest {
            name: cluster,
            blueprint_name: blueprint,
        };
        let response =
            match timeout(Duration::from_secs(5), client.list_jobs(list_jobs_request)).await {
                Ok(Ok(res)) => res.into_inner(),
                Ok(Err(status)) => return Err(app_error_from_status(status)),
                Err(e) => return Err(AppError::network_error(format!("operation timed out: {e}"))),
            };
        Ok(response.jobs)
    }

    async fn list_partitions(&self, name: &str) -> AppResult<Vec<String>> {
        let mut client = self.connect().await?;
        let request = ListPartitionsRequest {
            name: name.to_string(),
        };
        let response = match timeout(Duration::from_secs(5), client.list_partitions(request)).await
        {
            Ok(Ok(res)) => res.into_inner(),
            Ok(Err(status)) => return Err(app_error_from_status(status)),
            Err(e) => return Err(AppError::network_error(format!("operation timed out: {e}"))),
        };
        Ok(response
            .partitions
            .into_iter()
            .map(|item| item.name)
            .collect())
    }

    async fn list_accounts(&self, name: &str) -> AppResult<Vec<String>> {
        let mut client = self.connect().await?;
        let request = ListAccountsRequest {
            name: name.to_string(),
        };
        let response = match timeout(Duration::from_secs(5), client.list_accounts(request)).await {
            Ok(Ok(res)) => res.into_inner(),
            Ok(Err(status)) => return Err(app_error_from_status(status)),
            Err(e) => return Err(AppError::network_error(format!("operation timed out: {e}"))),
        };
        Ok(response
            .accounts
            .into_iter()
            .map(|item| item.name)
            .collect())
    }

    async fn upsert_blueprint(&self, name: &str, path: &str) -> AppResult<proto::BlueprintRecord> {
        let mut client = self.connect().await?;
        let request = UpsertBlueprintRequest {
            name: name.to_string(),
            path: path.to_string(),
        };
        let response = match timeout(Duration::from_secs(5), client.upsert_blueprint(request)).await
        {
            Ok(Ok(res)) => res.into_inner(),
            Ok(Err(status)) => return Err(app_error_from_status(status)),
            Err(e) => return Err(AppError::network_error(format!("operation timed out: {e}"))),
        };
        response
            .blueprint
            .ok_or_else(|| AppError::remote_error("missing blueprint in response"))
    }

    async fn get_blueprint(&self, name: &str) -> AppResult<proto::BlueprintRecord> {
        let mut client = self.connect().await?;
        let request = GetBlueprintRequest {
            name: name.to_string(),
        };
        let response = match timeout(Duration::from_secs(5), client.get_blueprint(request)).await {
            Ok(Ok(res)) => res.into_inner(),
            Ok(Err(status)) => return Err(app_error_from_status(status)),
            Err(e) => return Err(AppError::network_error(format!("operation timed out: {e}"))),
        };
        response
            .blueprint
            .ok_or_else(|| AppError::remote_error("missing blueprint in response"))
    }

    async fn list_blueprints(&self) -> AppResult<Vec<proto::BlueprintRecord>> {
        let mut client = self.connect().await?;
        let request = ListBlueprintsRequest {};
        let response = match timeout(Duration::from_secs(5), client.list_blueprints(request)).await
        {
            Ok(Ok(res)) => res.into_inner(),
            Ok(Err(status)) => return Err(app_error_from_status(status)),
            Err(e) => return Err(AppError::network_error(format!("operation timed out: {e}"))),
        };
        Ok(response.blueprints)
    }

    async fn delete_blueprint(&self, name: &str) -> AppResult<bool> {
        let mut client = self.connect().await?;
        let request = DeleteBlueprintRequest {
            name: name.to_string(),
        };
        let response = match timeout(Duration::from_secs(5), client.delete_blueprint(request)).await
        {
            Ok(Ok(res)) => res.into_inner(),
            Ok(Err(status)) => return Err(app_error_from_status(status)),
            Err(e) => return Err(AppError::network_error(format!("operation timed out: {e}"))),
        };
        Ok(response.deleted)
    }

    async fn build_blueprint(
        &self,
        path: String,
        package_git: bool,
    ) -> AppResult<proto::BlueprintRecord> {
        let mut client = self.connect().await?;
        let request = BuildBlueprintRequest { path, package_git };
        let response = match timeout(Duration::from_secs(10), client.build_blueprint(request)).await
        {
            Ok(Ok(res)) => res.into_inner(),
            Ok(Err(status)) => return Err(app_error_from_status(status)),
            Err(e) => return Err(AppError::network_error(format!("operation timed out: {e}"))),
        };
        response
            .blueprint
            .ok_or_else(|| AppError::remote_error("missing blueprint in response"))
    }

    async fn delete_cluster(&self, name: &str, force: bool) -> AppResult<bool> {
        let mut client = self.connect().await?;
        let delete_request = DeleteClusterRequest {
            name: name.to_string(),
            force,
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

    async fn run_job(
        &self,
        name: String,
        local_path: String,
        remote_path: Option<String>,
        new_directory: bool,
        sbatchscript: String,
        filters: Vec<RunPathFilterRule>,
        blueprint_name: Option<String>,
        default_retrieve_path: Option<String>,
        template_values_json: Option<String>,
        output: &mut dyn StreamOutputPort,
        interaction: &dyn InteractionPort,
    ) -> AppResult<RunCapture> {
        let mut client = self.connect().await?;
        let (tx_ans, rx_ans) = mpsc::channel::<RunJobRequest>(16);
        let outbound = ReceiverStream::new(rx_ans);
        tx_ans
            .send(RunJobRequest {
                msg: Some(run_job_request::Msg::Init(proto::RunJobRequestInit {
                    local_path,
                    remote_path,
                    name,
                    sbatchscript,
                    filters,
                    new_directory,
                    blueprint_name,
                    default_retrieve_path,
                    template_values_json,
                })),
            })
            .await
            .map_err(|_| AppError::internal_error("failed to send run init"))?;

        let response = match client.run_job(Request::new(outbound)).await {
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
        handle_run_stream(inbound, output, interaction, move |answers| {
            let tx_ans = tx_ans.clone();
            async move {
                tx_ans
                    .send(RunJobRequest {
                        msg: Some(run_job_request::Msg::Mfa(answers)),
                    })
                    .await
                    .map_err(|_| AppError::remote_error("server closed while sending MFA answers"))
            }
        })
        .await
    }

    async fn run_blueprint(
        &self,
        blueprint_name: String,
        blueprint_tag: String,
        name: String,
        remote_path: Option<String>,
        new_directory: bool,
        sbatchscript: String,
        filters: Vec<RunPathFilterRule>,
        default_retrieve_path: Option<String>,
        template_values_json: Option<String>,
        output: &mut dyn StreamOutputPort,
        interaction: &dyn InteractionPort,
    ) -> AppResult<RunCapture> {
        let mut client = self.connect().await?;
        let (tx_ans, rx_ans) = mpsc::channel::<RunBlueprintRequest>(16);
        let outbound = ReceiverStream::new(rx_ans);
        tx_ans
            .send(RunBlueprintRequest {
                msg: Some(run_blueprint_request::Msg::Init(
                    proto::RunBlueprintRequestInit {
                        blueprint_name,
                        blueprint_tag,
                        name,
                        sbatchscript,
                        filters,
                        new_directory,
                        remote_path,
                        default_retrieve_path,
                        template_values_json,
                    },
                )),
            })
            .await
            .map_err(|_| AppError::internal_error("failed to send run init"))?;

        let response = match client.run_blueprint(Request::new(outbound)).await {
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
        handle_run_stream(inbound, output, interaction, move |answers| {
            let tx_ans = tx_ans.clone();
            async move {
                tx_ans
                    .send(RunBlueprintRequest {
                        msg: Some(run_blueprint_request::Msg::Mfa(answers)),
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
        planned_is_default: Option<bool>,
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
        let init_default_base_path = if interactive_scratch_selection {
            None
        } else {
            default_base_path.clone()
        };
        let init = AddClusterInit {
            name,
            username,
            host: Some(host),
            identity_path: identity_path_expanded,
            port,
            default_base_path: init_default_base_path,
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
        let mut selected_default_base_path = default_base_path.clone();
        let mut provided_base_path = if interactive_scratch_selection {
            default_base_path.clone()
        } else {
            None
        };
        let mut base_path_default = default_base_path.clone();
        let mut pending_base_path_prompt: Option<PromptLine> = None;
        let mut pending_base_path_input: Option<String> = None;
        let mut selected_scratch_directory = default_scratch_directory;
        let mut selected_is_default = planned_is_default;
        let mut scratch_options: Vec<String> = Vec::new();
        let gathering_spinner_message = "Gathering additional cluster information...";
        let mut gathering_feedback: Option<Box<dyn PromptFeedbackPort>> =
            if interactive_scratch_selection && selected_scratch_directory.is_none() {
                Some(interaction.prompt_feedback().await?)
            } else {
                None
            };
        let mut gathering_active = false;
        let mut active_validation_spinner_message: Option<String> = None;
        let mut pending_validation_path: Option<String> = None;
        let mut validation_spinner: Option<InlineSpinner> = None;
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
                        if let Some(mut active_spinner) = validation_spinner.take() {
                            active_spinner.stop();
                        }
                        if gathering_active {
                            if let Some(feedback) = gathering_feedback.as_mut() {
                                feedback.stop_information_gathering()?;
                            }
                            gathering_active = false;
                        }
                        output.on_exit_code(code).await?;
                        break;
                    }
                    stream_event::Event::Mfa(mfa) => {
                        if let Some(mut active_spinner) = validation_spinner.take() {
                            active_spinner.stop();
                        }
                        let validation_spinner_active_message =
                            active_validation_spinner_message.clone();
                        let gathering_was_active = gathering_active;
                        if gathering_was_active {
                            if let Some(feedback) = gathering_feedback.as_mut() {
                                feedback.stop_information_gathering()?;
                            }
                        }
                        let base_path_validation_active =
                            if let Some(base_path_prompt) = pending_base_path_prompt.as_mut() {
                                base_path_prompt.stop_validation()?;
                                true
                            } else {
                                false
                            };
                        let (answers, mfa_lines) = interaction.prompt_mfa_transient(&mfa).await?;
                        tx_ans
                            .send(AddClusterRequest {
                                msg: Some(proto::add_cluster_request::Msg::Mfa(answers)),
                            })
                            .await
                            .map_err(|_| {
                                AppError::remote_error("server closed while sending MFA answers")
                            })?;
                        if mfa_lines > 0 {
                            interaction.clear_transient(mfa_lines).await?;
                        }
                        if base_path_validation_active {
                            if let Some(base_path_prompt) = pending_base_path_prompt.as_mut() {
                                base_path_prompt
                                    .start_validation("Validating default base path...")?;
                            }
                        } else if let Some(message) = validation_spinner_active_message.as_deref() {
                            validation_spinner = InlineSpinner::start(message);
                        }
                        if gathering_was_active {
                            if let Some(feedback) = gathering_feedback.as_mut() {
                                feedback.start_information_gathering(gathering_spinner_message)?;
                            }
                        }
                    }
                    stream_event::Event::Error(err) => {
                        if let Some(mut active_spinner) = validation_spinner.take() {
                            active_spinner.stop();
                        }
                        if gathering_active {
                            if let Some(feedback) = gathering_feedback.as_mut() {
                                feedback.stop_information_gathering()?;
                            }
                            gathering_active = false;
                        }
                        output.on_error(&err).await?;
                        output.on_exit_code(1).await?;
                        break;
                    }
                    stream_event::Event::AddClusterScratchOptions(options) => {
                        if let Some(mut active_spinner) = validation_spinner.take() {
                            active_spinner.stop();
                        }
                        if gathering_active {
                            if let Some(feedback) = gathering_feedback.as_mut() {
                                feedback.stop_information_gathering()?;
                            }
                            gathering_active = false;
                        }
                        scratch_options = options.directories;
                        let selection =
                            prompt_add_cluster_scratch_selection(interaction, &scratch_options)
                                .await?;
                        pending_validation_path = selection.clone();
                        if let Some(path) = selection.as_deref() {
                            let message = format!("Validating {path}...");
                            active_validation_spinner_message = Some(message.clone());
                            validation_spinner = InlineSpinner::start(&message);
                        } else {
                            active_validation_spinner_message = None;
                        }
                        send_add_cluster_scratch_selection(&tx_ans, selection).await?;
                    }
                    stream_event::Event::AddClusterScratchValidation(validation) => {
                        if let Some(mut active_spinner) = validation_spinner.take() {
                            active_spinner.stop();
                        }
                        active_validation_spinner_message = None;
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
                            let failure_reason = validation
                                .error
                                .as_deref()
                                .filter(|value| !value.trim().is_empty())
                                .unwrap_or("validation failed");
                            let failure_line =
                                format!("✗ Validation of {failed_path} failed: {failure_reason}\n");
                            output.on_stderr(failure_line.as_bytes()).await?;
                            let selection =
                                prompt_add_cluster_scratch_selection(interaction, &scratch_options)
                                    .await?;
                            pending_validation_path = selection.clone();
                            if let Some(path) = selection.as_deref() {
                                let message = format!("Validating {path}...");
                                active_validation_spinner_message = Some(message.clone());
                                validation_spinner = InlineSpinner::start(&message);
                            } else {
                                active_validation_spinner_message = None;
                            }
                            send_add_cluster_scratch_selection(&tx_ans, selection).await?;
                        }
                    }
                    stream_event::Event::AddClusterBasePathPrompt(prompt) => {
                        if let Some(mut active_spinner) = validation_spinner.take() {
                            active_spinner.stop();
                        }
                        let suggested_default = prompt.suggested_path.trim();
                        let prompt_default = if suggested_default.is_empty() {
                            base_path_default
                                .clone()
                                .unwrap_or_else(|| "~/runs".to_string())
                        } else {
                            suggested_default.to_string()
                        };
                        base_path_default = Some(prompt_default.clone());
                        if let Some(path) = provided_base_path.take() {
                            let base_path = path.trim().to_string();
                            let message = "Validating default base path...".to_string();
                            active_validation_spinner_message = Some(message.clone());
                            validation_spinner = InlineSpinner::start(&message);
                            pending_base_path_input = Some(base_path.clone());
                            send_add_cluster_base_path_selection(&tx_ans, base_path).await?;
                            continue;
                        }
                        let mut base_path_prompt =
                            prompt_add_cluster_default_base_path(interaction, &prompt_default)
                                .await?;
                        let base_path = base_path_prompt.input.trim().to_string();
                        base_path_prompt.start_validation("Validating default base path...")?;
                        pending_base_path_input = Some(base_path.clone());
                        pending_base_path_prompt = Some(base_path_prompt);
                        send_add_cluster_base_path_selection(&tx_ans, base_path).await?;
                    }
                    stream_event::Event::AddClusterBasePathValidation(validation) => {
                        if let Some(mut active_spinner) = validation_spinner.take() {
                            active_spinner.stop();
                        }
                        active_validation_spinner_message = None;
                        let mut prompt = pending_base_path_prompt.take();
                        if validation.accepted {
                            let validated_path = validation
                                .path
                                .clone()
                                .or_else(|| pending_base_path_input.clone())
                                .unwrap_or_else(|| "<default base path>".to_string());
                            if let Some(active_prompt) = prompt.as_mut() {
                                active_prompt.finish_success(&format!(
                                    "Default base path: {validated_path}"
                                ))?;
                            } else {
                                output
                                    .on_stderr(
                                        format!("✓ Default base path: {validated_path}\n")
                                            .as_bytes(),
                                    )
                                    .await?;
                            }
                            selected_default_base_path = Some(validated_path);
                            pending_base_path_input = None;
                            if !gathering_active {
                                if let Some(feedback) = gathering_feedback.as_mut() {
                                    feedback
                                        .start_information_gathering(gathering_spinner_message)?;
                                    gathering_active = true;
                                }
                            }
                        } else {
                            let failure_reason = validation
                                .error
                                .as_deref()
                                .filter(|value| !value.trim().is_empty())
                                .unwrap_or("validation failed");
                            if let Some(active_prompt) = prompt.as_mut() {
                                active_prompt.finish_failure(failure_reason)?;
                            } else {
                                let failed_path = pending_base_path_input
                                    .as_deref()
                                    .unwrap_or("<default base path>");
                                output
                                    .on_stderr(
                                        format!(
                                            "✗ Validation of {failed_path} failed: {failure_reason}\n"
                                        )
                                        .as_bytes(),
                                    )
                                    .await?;
                            }
                            let next_default =
                                base_path_default.as_deref().unwrap_or("~/runs").to_string();
                            let mut next_prompt =
                                prompt_add_cluster_default_base_path(interaction, &next_default)
                                    .await?;
                            let next_path = next_prompt.input.trim().to_string();
                            next_prompt.start_validation("Validating default base path...")?;
                            pending_base_path_input = Some(next_path.clone());
                            pending_base_path_prompt = Some(next_prompt);
                            send_add_cluster_base_path_selection(&tx_ans, next_path).await?;
                        }
                    }
                    stream_event::Event::AddClusterDefaultPrompt(_prompt) => {
                        if let Some(mut active_spinner) = validation_spinner.take() {
                            active_spinner.stop();
                        }
                        let gathering_was_active = gathering_active;
                        if gathering_was_active {
                            if let Some(feedback) = gathering_feedback.as_mut() {
                                feedback.stop_information_gathering()?;
                            }
                            gathering_active = false;
                        }

                        let make_default = if let Some(value) = planned_is_default {
                            value
                        } else {
                            interaction
                                .confirm(
                                    "Set this cluster as the default? (yes/no): ",
                                    "Type yes to confirm, no to cancel.",
                                )
                                .await?
                        };
                        selected_is_default = Some(make_default);
                        send_add_cluster_default_selection(&tx_ans, make_default).await?;
                        let label = if make_default { "yes" } else { "no" };
                        output
                            .on_stderr(format!("✓ Default cluster: {label}\n").as_bytes())
                            .await?;

                        if gathering_was_active {
                            if let Some(feedback) = gathering_feedback.as_mut() {
                                feedback.start_information_gathering(gathering_spinner_message)?;
                                gathering_active = true;
                            }
                        }
                    }
                },
                Ok(proto::StreamEvent { event: None }) => {}
                Err(status) => {
                    if let Some(mut active_spinner) = validation_spinner.take() {
                        active_spinner.stop();
                    }
                    if gathering_active {
                        if let Some(feedback) = gathering_feedback.as_mut() {
                            feedback.stop_information_gathering()?;
                        }
                        gathering_active = false;
                    }
                    output
                        .on_error(remote_code_for_status(status.code()))
                        .await?;
                    output.on_exit_code(1).await?;
                    break;
                }
            }
        }
        if let Some(mut active_spinner) = validation_spinner.take() {
            active_spinner.stop();
        }
        if gathering_active {
            if let Some(feedback) = gathering_feedback.as_mut() {
                feedback.stop_information_gathering()?;
            }
        }

        Ok(AddClusterCapture {
            stream: output.take_stream_capture(),
            default_base_path: selected_default_base_path,
            default_scratch_directory: selected_scratch_directory,
            is_default: selected_is_default,
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

async fn prompt_add_cluster_default_base_path(
    interaction: &dyn InteractionPort,
    default: &str,
) -> AppResult<PromptLine> {
    interaction
        .prompt_line_with_default_confirmable(
            "Default base path: ",
            "Remote base folder for runs.",
            default,
        )
        .await
}

async fn send_add_cluster_base_path_selection(
    sender: &mpsc::Sender<AddClusterRequest>,
    path: String,
) -> AppResult<()> {
    sender
        .send(AddClusterRequest {
            msg: Some(add_cluster_request::Msg::BasePathSelection(
                proto::AddClusterBasePathSelection { path },
            )),
        })
        .await
        .map_err(|_| AppError::remote_error("server closed while sending default base path"))
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

async fn send_add_cluster_default_selection(
    sender: &mpsc::Sender<AddClusterRequest>,
    is_default: bool,
) -> AppResult<()> {
    sender
        .send(AddClusterRequest {
            msg: Some(add_cluster_request::Msg::DefaultSelection(
                proto::AddClusterDefaultSelection { is_default },
            )),
        })
        .await
        .map_err(|_| AppError::remote_error("server closed while sending default selection"))
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
                let remote_code = remote_code_for_status(status.code());
                output.on_error(remote_code).await?;
                output.on_exit_code(map_error_code(remote_code)).await?;
                break;
            }
        }
    }

    Ok(output.take_stream_capture())
}

async fn handle_run_stream<S, F, Fut>(
    mut inbound: S,
    output: &mut dyn StreamOutputPort,
    interaction: &dyn InteractionPort,
    mut send_mfa: F,
) -> AppResult<RunCapture>
where
    S: tokio_stream::Stream<Item = Result<proto::RunStreamEvent, Status>> + Unpin,
    F: FnMut(proto::MfaAnswer) -> Fut,
    Fut: Future<Output = AppResult<()>>,
{
    while let Some(item) = inbound.next().await {
        match item {
            Ok(proto::RunStreamEvent { event: Some(ev) }) => match ev {
                run_stream_event::Event::Stdout(bytes) => {
                    output.on_stdout(&bytes).await?;
                }
                run_stream_event::Event::Stderr(bytes) => {
                    output.on_stderr(&bytes).await?;
                }
                run_stream_event::Event::ExitCode(code) => {
                    output.on_exit_code(code).await?;
                    break;
                }
                run_stream_event::Event::Mfa(mfa) => {
                    let answers = interaction.prompt_mfa(&mfa).await?;
                    send_mfa(answers).await?;
                }
                run_stream_event::Event::Error(err) => {
                    output.on_error(&err).await?;
                    output.on_exit_code(1).await?;
                    break;
                }
                run_stream_event::Event::RunStatus(status) => {
                    output.on_run_status(&status).await?;
                }
                run_stream_event::Event::RunResult(result) => {
                    output.on_run_result(&result).await?;
                    break;
                }
            },
            Ok(proto::RunStreamEvent { event: None }) => {}
            Err(status) => {
                output
                    .on_error(remote_code_for_status(status.code()))
                    .await?;
                output.on_exit_code(1).await?;
                break;
            }
        }
    }

    Ok(output.take_run_capture())
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
