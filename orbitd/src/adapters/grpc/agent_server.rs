// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;

use async_trait::async_trait;
use proto::agent_server::Agent;
use proto::{
    AddClusterRequest, BuildBlueprintRequest, BuildBlueprintResponse, CancelJobRequest,
    CleanupJobRequest, DeleteBlueprintRequest, DeleteBlueprintResponse, DeleteClusterRequest,
    DeleteClusterResponse, GetBlueprintRequest, GetBlueprintResponse, JobLogsRequest,
    ListAccountsRequest, ListAccountsResponse, ListAccountsUnitResponse, ListBlueprintsRequest,
    ListBlueprintsResponse, ListClustersRequest, ListClustersResponse, ListJobsRequest,
    ListJobsResponse, ListPartitionsRequest, ListPartitionsResponse, ListPartitionsUnitResponse,
    LsRequest, MfaAnswer, PingReply, PingRequest, ReconnectClusterRequest,
    ReconnectClusterStreamEvent, ResolveHomeDirRequest, RetrieveJobRequest, RunBlueprintRequest,
    RunJobRequest, RunStreamEvent, SetClusterRequest, StreamEvent, UpsertBlueprintRequest,
    UpsertBlueprintResponse, reconnect_cluster_stream_event, stream_event,
};
use tokio::sync::mpsc;
use tokio_stream::Stream;
use tonic::{Request, Response, Status};
use tracing::Instrument;

use crate::adapters::grpc::mapping::{
    blueprint_record_to_response, build_sync_filters, cluster_status_to_response,
    job_record_to_response,
};
use crate::app::errors::{AppError, AppErrorKind, codes};
use crate::app::ports::{MfaPort, RunStreamOutputPort, StreamOutputPort};
use crate::app::types::Address;
use crate::app::usecases::{
    AddClusterInput, CancelJobInput, CleanupJobInput, JobLogsInput, LsInput, ResolveHomeDirInput,
    ResolveScratchDirectoriesInput, RetrieveJobInput, RunBlueprintInput, RunJobInput,
    SetClusterInput, UseCases, ValidateDefaultBasePathInput, ValidateScratchDirectoryInput,
};

pub type OutStream =
    Pin<Box<dyn Stream<Item = Result<StreamEvent, Status>> + Send + Sync + 'static>>;
pub type RunOutStream =
    Pin<Box<dyn Stream<Item = Result<RunStreamEvent, Status>> + Send + Sync + 'static>>;
pub type ReconnectClusterOutStream = Pin<
    Box<dyn Stream<Item = Result<ReconnectClusterStreamEvent, Status>> + Send + Sync + 'static>,
>;

#[derive(Clone)]
pub struct GrpcAgent {
    usecases: UseCases,
}

impl GrpcAgent {
    pub fn new(usecases: UseCases) -> Self {
        Self { usecases }
    }
}

#[derive(Clone)]
struct GrpcStreamOutput {
    sender: mpsc::Sender<Result<StreamEvent, Status>>,
}

#[async_trait]
impl StreamOutputPort for GrpcStreamOutput {
    async fn send(&self, event: StreamEvent) -> Result<(), AppError> {
        self.sender
            .send(Ok(event))
            .await
            .map_err(|_| AppError::new(AppErrorKind::Cancelled, codes::CANCELED))
    }

    fn is_closed(&self) -> bool {
        self.sender.is_closed()
    }
}

#[derive(Clone)]
struct GrpcRunStreamOutput {
    sender: mpsc::Sender<Result<RunStreamEvent, Status>>,
}

#[async_trait]
impl RunStreamOutputPort for GrpcRunStreamOutput {
    async fn send(&self, event: RunStreamEvent) -> Result<(), AppError> {
        self.sender
            .send(Ok(event))
            .await
            .map_err(|_| AppError::new(AppErrorKind::Cancelled, codes::CANCELED))
    }

    fn is_closed(&self) -> bool {
        self.sender.is_closed()
    }
}

#[derive(Clone)]
struct GrpcReconnectClusterStreamOutput {
    sender: mpsc::Sender<Result<ReconnectClusterStreamEvent, Status>>,
}

#[async_trait]
impl StreamOutputPort for GrpcReconnectClusterStreamOutput {
    async fn send(&self, event: StreamEvent) -> Result<(), AppError> {
        let mapped = match event.event {
            Some(stream_event::Event::Mfa(mfa)) => Some(ReconnectClusterStreamEvent {
                event: Some(reconnect_cluster_stream_event::Event::Mfa(mfa)),
            }),
            _ => None,
        };

        if let Some(event) = mapped {
            self.sender
                .send(Ok(event))
                .await
                .map_err(|_| AppError::new(AppErrorKind::Cancelled, codes::CANCELED))?;
        }
        Ok(())
    }

    fn is_closed(&self) -> bool {
        self.sender.is_closed()
    }
}
/// gRPC-specific MFA port implementation. It will be owning
/// the mpsc receiver of MfaAnswer
struct GrpcMfaPort {
    receiver: mpsc::Receiver<MfaAnswer>,
}

impl MfaPort for GrpcMfaPort {
    fn receiver(&mut self) -> &mut mpsc::Receiver<MfaAnswer> {
        &mut self.receiver
    }
}

fn status_from_app_error(err: AppError) -> Status {
    match err.kind() {
        AppErrorKind::InvalidArgument => Status::invalid_argument(err.message()),
        AppErrorKind::NotFound => Status::not_found(err.message()),
        AppErrorKind::Conflict => Status::already_exists(err.message()),
        AppErrorKind::AlreadyExists => Status::already_exists(err.message()),
        AppErrorKind::Internal => Status::internal(err.message()),
        AppErrorKind::Aborted => Status::aborted(err.message()),
        AppErrorKind::Cancelled => Status::cancelled(err.message()),
        AppErrorKind::Unknown => Status::unknown(err.message()),
    }
}

fn format_remote_addr(addr: Option<SocketAddr>) -> String {
    addr.map(|value| value.to_string())
        .unwrap_or_else(|| "unknown".to_string())
}

const REQUEST_ID_HEADER: &str = "x-orbit-request-id";

fn request_id_from_metadata(metadata: &tonic::metadata::MetadataMap) -> String {
    metadata
        .get(REQUEST_ID_HEADER)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| value.to_string())
        .unwrap_or_else(|| format!("{:032x}", rand::random::<u128>()))
}

fn rpc_span<T>(request: &Request<T>, method: &'static str) -> tracing::Span {
    let peer = format_remote_addr(request.remote_addr());
    let request_id = request_id_from_metadata(request.metadata());
    tracing::info_span!("rpc", method = %method, peer = %peer, request_id = %request_id)
}

fn parse_add_cluster_host(host: Option<proto::add_cluster_init::Host>) -> Result<Address, Status> {
    let host = match host {
        Some(v) => v,
        None => return Err(Status::invalid_argument(codes::INVALID_ARGUMENT)),
    };
    match host {
        proto::add_cluster_init::Host::Hostname(v) => Ok(Address::Hostname(v)),
        proto::add_cluster_init::Host::Ipaddr(addr) => {
            let ip: IpAddr = match addr.parse() {
                Ok(v) => v,
                Err(_) => {
                    return Err(Status::invalid_argument(codes::INVALID_ARGUMENT));
                }
            };
            Ok(Address::Ip(ip))
        }
    }
}

fn parse_resolve_home_host(
    host: Option<proto::resolve_home_dir_request_init::Host>,
) -> Result<Address, Status> {
    let host = match host {
        Some(v) => v,
        None => return Err(Status::invalid_argument(codes::INVALID_ARGUMENT)),
    };
    match host {
        proto::resolve_home_dir_request_init::Host::Hostname(v) => Ok(Address::Hostname(v)),
        proto::resolve_home_dir_request_init::Host::Ipaddr(addr) => {
            let ip: IpAddr = match addr.parse() {
                Ok(v) => v,
                Err(_) => {
                    return Err(Status::invalid_argument(codes::INVALID_ARGUMENT));
                }
            };
            Ok(Address::Ip(ip))
        }
    }
}

fn parse_set_cluster_host(host: &str) -> Result<Address, Status> {
    let trimmed = host.trim();
    if trimmed.is_empty() {
        return Err(Status::invalid_argument(codes::INVALID_ARGUMENT));
    }
    match trimmed.parse::<IpAddr>() {
        Ok(ip) => Ok(Address::Ip(ip)),
        Err(_) => Ok(Address::Hostname(trimmed.to_string())),
    }
}

fn parse_port(port: u32) -> Result<u16, Status> {
    match u16::try_from(port) {
        Ok(v) => Ok(v),
        Err(_) => Err(Status::invalid_argument(codes::INVALID_ARGUMENT)),
    }
}

#[tonic::async_trait]
impl Agent for GrpcAgent {
    type LsStream = OutStream;
    type RetrieveJobStream = OutStream;
    type JobLogsStream = OutStream;
    type CancelJobStream = OutStream;
    type CleanupJobStream = OutStream;
    type RunJobStream = RunOutStream;
    type RunBlueprintStream = RunOutStream;
    type AddClusterStream = OutStream;
    type SetClusterStream = OutStream;
    type ReconnectClusterStream = ReconnectClusterOutStream;
    type ResolveHomeDirStream = OutStream;

    async fn ping(&self, request: Request<PingRequest>) -> Result<Response<PingReply>, Status> {
        let span = rpc_span(&request, "Ping");
        let _enter = span.enter();
        tracing::info!(target: "orbitd::rpc", "received request");
        let remote_addr = format_remote_addr(request.remote_addr());
        let req = request.into_inner();
        match self.usecases.ping(&req.message).await {
            Ok(message) => {
                tracing::info!("ping remote_addr={remote_addr}");
                Ok(Response::new(PingReply { message }))
            }
            Err(err) => {
                tracing::warn!(
                    "ping rejected remote_addr={remote_addr} message={}",
                    req.message
                );
                Err(status_from_app_error(err))
            }
        }
    }

    async fn resolve_home_dir(
        &self,
        request: Request<tonic::Streaming<ResolveHomeDirRequest>>,
    ) -> Result<Response<Self::ResolveHomeDirStream>, Status> {
        let span = rpc_span(&request, "ResolveHomeDir");
        let _enter = span.enter();
        tracing::info!(target: "orbitd::rpc", "received request");
        let remote_addr = format_remote_addr(request.remote_addr());
        let mut inbound = request.into_inner();
        let init = inbound
            .message()
            .await
            .map_err(|_| Status::unknown(codes::INTERNAL_ERROR))?
            .ok_or_else(|| Status::invalid_argument(codes::INVALID_ARGUMENT))?;

        let (username, host, identity_path, port, name) = match init.msg {
            Some(proto::resolve_home_dir_request::Msg::Init(i)) => {
                (i.username, i.host, i.identity_path, i.port, i.name)
            }
            _ => {
                return Err(Status::invalid_argument(codes::INVALID_ARGUMENT));
            }
        };

        let address = parse_resolve_home_host(host)?;
        let port = parse_port(port)?;
        let session_name = name.and_then(|value| {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        });
        tracing::info!(
            "resolve_home_dir start remote_addr={remote_addr} session_name={} username={} host={:?} port={port}",
            session_name.as_deref().unwrap_or("<none>"),
            username,
            address
        );
        let current_span = tracing::Span::current();

        let (evt_tx, evt_rx) = mpsc::channel::<Result<StreamEvent, Status>>(64);
        let (mfa_tx, mfa_rx) = mpsc::channel::<MfaAnswer>(16);
        tokio::spawn(
            async move {
                while let Ok(Some(item)) = inbound.message().await {
                    if let Some(proto::resolve_home_dir_request::Msg::Mfa(ans)) = item.msg
                        && mfa_tx.send(ans).await.is_err()
                    {
                        break;
                    }
                }
            }
            .instrument(current_span.clone()),
        );

        let mut mfa_port = GrpcMfaPort { receiver: mfa_rx };
        let stream_output = GrpcStreamOutput {
            sender: evt_tx.clone(),
        };
        let usecases = self.usecases.clone();
        let input = ResolveHomeDirInput {
            username,
            address,
            port,
            identity_path,
            session_name,
        };
        tokio::spawn(
            async move {
                if let Err(err) = usecases
                    .resolve_home_dir(input, &stream_output, &mut mfa_port)
                    .await
                {
                    let _ = evt_tx.send(Err(status_from_app_error(err))).await;
                }
            }
            .instrument(current_span.clone()),
        );

        let out: OutStream = Box::pin(crate::adapters::ssh::receiver_to_stream(evt_rx));
        Ok(Response::new(out))
    }

    async fn ls(
        &self,
        request: Request<tonic::Streaming<LsRequest>>,
    ) -> Result<Response<Self::LsStream>, Status> {
        let span = rpc_span(&request, "Ls");
        let _enter = span.enter();
        tracing::info!(target: "orbitd::rpc", "received request");
        let remote_addr = format_remote_addr(request.remote_addr());
        let mut inbound = request.into_inner();
        let init = inbound
            .message()
            .await
            .map_err(|_| Status::unknown(codes::INTERNAL_ERROR))?
            .ok_or_else(|| Status::invalid_argument(codes::INVALID_ARGUMENT))?;

        let (name, path, job_id) = match init.msg {
            Some(proto::ls_request::Msg::Init(init)) => (init.name, init.path, init.job_id),
            _ => {
                return Err(Status::invalid_argument(codes::INVALID_ARGUMENT));
            }
        };
        tracing::info!(
            "ls start remote_addr={remote_addr} name={} job_id={:?} requested_path={:?}",
            name,
            job_id,
            path
        );
        let current_span = tracing::Span::current();

        let (evt_tx, evt_rx) = mpsc::channel::<Result<StreamEvent, Status>>(64);
        let (mfa_tx, mfa_rx) = mpsc::channel::<MfaAnswer>(16);
        tokio::spawn(
            async move {
                while let Ok(Some(item)) = inbound.message().await {
                    if let Some(proto::ls_request::Msg::Mfa(ans)) = item.msg
                        && mfa_tx.send(ans).await.is_err()
                    {
                        break;
                    }
                }
            }
            .instrument(current_span.clone()),
        );

        let input = LsInput { name, path, job_id };
        let mut mfa_port = GrpcMfaPort { receiver: mfa_rx };
        let stream_output = GrpcStreamOutput {
            sender: evt_tx.clone(),
        };
        let usecases = self.usecases.clone();
        tokio::spawn(
            async move {
                if let Err(err) = usecases.ls(input, &stream_output, &mut mfa_port).await {
                    let _ = evt_tx.send(Err(status_from_app_error(err))).await;
                }
            }
            .instrument(current_span.clone()),
        );

        let out: OutStream = Box::pin(crate::adapters::ssh::receiver_to_stream(evt_rx));
        Ok(Response::new(out))
    }

    async fn retrieve_job(
        &self,
        request: Request<tonic::Streaming<RetrieveJobRequest>>,
    ) -> Result<Response<Self::RetrieveJobStream>, Status> {
        let span = rpc_span(&request, "RetrieveJob");
        let _enter = span.enter();
        tracing::info!(target: "orbitd::rpc", "received request");
        let remote_addr = format_remote_addr(request.remote_addr());
        let mut inbound = request.into_inner();
        let init = inbound
            .message()
            .await
            .map_err(|_| Status::unknown(codes::INTERNAL_ERROR))?
            .ok_or_else(|| Status::invalid_argument(codes::INVALID_ARGUMENT))?;

        let (job_id, path, local_path, overwrite, force) = match init.msg {
            Some(proto::retrieve_job_request::Msg::Init(init)) => (
                init.job_id,
                init.path,
                init.local_path,
                init.overwrite,
                init.force,
            ),
            _ => return Err(Status::invalid_argument(codes::INVALID_ARGUMENT)),
        };
        tracing::info!(
            "retrieve_job start remote_addr={remote_addr} job_id={job_id} path={path} local_path={:?}",
            local_path
        );
        let current_span = tracing::Span::current();

        let (evt_tx, evt_rx) = mpsc::channel::<Result<StreamEvent, Status>>(64);
        let (mfa_tx, mfa_rx) = mpsc::channel::<MfaAnswer>(16);
        tokio::spawn(
            async move {
                while let Ok(Some(item)) = inbound.message().await {
                    if let Some(proto::retrieve_job_request::Msg::Mfa(ans)) = item.msg
                        && mfa_tx.send(ans).await.is_err()
                    {
                        break;
                    }
                }
            }
            .instrument(current_span.clone()),
        );

        let input = RetrieveJobInput {
            job_id,
            path,
            local_path,
            overwrite,
            force,
        };
        let mut mfa_port = GrpcMfaPort { receiver: mfa_rx };
        let stream_output = GrpcStreamOutput {
            sender: evt_tx.clone(),
        };
        let usecases = self.usecases.clone();
        tokio::spawn(
            async move {
                if let Err(err) = usecases
                    .retrieve_job(input, &stream_output, &mut mfa_port)
                    .await
                {
                    let _ = evt_tx.send(Err(status_from_app_error(err))).await;
                }
            }
            .instrument(current_span.clone()),
        );

        let out: OutStream = Box::pin(crate::adapters::ssh::receiver_to_stream(evt_rx));
        Ok(Response::new(out))
    }

    async fn job_logs(
        &self,
        request: Request<tonic::Streaming<JobLogsRequest>>,
    ) -> Result<Response<Self::JobLogsStream>, Status> {
        let span = rpc_span(&request, "JobLogs");
        let _enter = span.enter();
        tracing::info!(target: "orbitd::rpc", "received request");
        let remote_addr = format_remote_addr(request.remote_addr());
        let mut inbound = request.into_inner();
        let init = inbound
            .message()
            .await
            .map_err(|_| Status::unknown(codes::INTERNAL_ERROR))?
            .ok_or_else(|| Status::invalid_argument(codes::INVALID_ARGUMENT))?;

        let (job_id, stderr) = match init.msg {
            Some(proto::job_logs_request::Msg::Init(init)) => (init.job_id, init.stderr),
            _ => return Err(Status::invalid_argument(codes::INVALID_ARGUMENT)),
        };
        tracing::info!("job_logs start remote_addr={remote_addr} job_id={job_id} stderr={stderr}");
        let current_span = tracing::Span::current();

        let (evt_tx, evt_rx) = mpsc::channel::<Result<StreamEvent, Status>>(64);
        let (mfa_tx, mfa_rx) = mpsc::channel::<MfaAnswer>(16);
        tokio::spawn(
            async move {
                while let Ok(Some(item)) = inbound.message().await {
                    if let Some(proto::job_logs_request::Msg::Mfa(ans)) = item.msg
                        && mfa_tx.send(ans).await.is_err()
                    {
                        break;
                    }
                }
            }
            .instrument(current_span.clone()),
        );

        let input = JobLogsInput { job_id, stderr };
        let mut mfa_port = GrpcMfaPort { receiver: mfa_rx };
        let stream_output = GrpcStreamOutput {
            sender: evt_tx.clone(),
        };
        let usecases = self.usecases.clone();
        tokio::spawn(
            async move {
                if let Err(err) = usecases
                    .job_logs(input, &stream_output, &mut mfa_port)
                    .await
                {
                    let _ = evt_tx.send(Err(status_from_app_error(err))).await;
                }
            }
            .instrument(current_span.clone()),
        );

        let out: OutStream = Box::pin(crate::adapters::ssh::receiver_to_stream(evt_rx));
        Ok(Response::new(out))
    }

    async fn cancel_job(
        &self,
        request: Request<tonic::Streaming<CancelJobRequest>>,
    ) -> Result<Response<Self::CancelJobStream>, Status> {
        let span = rpc_span(&request, "CancelJob");
        let _enter = span.enter();
        tracing::info!(target: "orbitd::rpc", "received request");
        let remote_addr = format_remote_addr(request.remote_addr());
        let mut inbound = request.into_inner();
        let init = inbound
            .message()
            .await
            .map_err(|_| Status::unknown(codes::INTERNAL_ERROR))?
            .ok_or_else(|| Status::invalid_argument(codes::INVALID_ARGUMENT))?;

        let job_id = match init.msg {
            Some(proto::cancel_job_request::Msg::Init(init)) => init.job_id,
            _ => return Err(Status::invalid_argument(codes::INVALID_ARGUMENT)),
        };
        tracing::info!("cancel_job start remote_addr={remote_addr} job_id={job_id}");
        let current_span = tracing::Span::current();

        let (evt_tx, evt_rx) = mpsc::channel::<Result<StreamEvent, Status>>(64);
        let (mfa_tx, mfa_rx) = mpsc::channel::<MfaAnswer>(16);
        tokio::spawn(
            async move {
                while let Ok(Some(item)) = inbound.message().await {
                    if let Some(proto::cancel_job_request::Msg::Mfa(ans)) = item.msg
                        && mfa_tx.send(ans).await.is_err()
                    {
                        break;
                    }
                }
            }
            .instrument(current_span.clone()),
        );

        let input = CancelJobInput { job_id };
        let mut mfa_port = GrpcMfaPort { receiver: mfa_rx };
        let stream_output = GrpcStreamOutput {
            sender: evt_tx.clone(),
        };
        let usecases = self.usecases.clone();
        tokio::spawn(
            async move {
                if let Err(err) = usecases
                    .cancel_job(input, &stream_output, &mut mfa_port)
                    .await
                {
                    let _ = evt_tx.send(Err(status_from_app_error(err))).await;
                }
            }
            .instrument(current_span.clone()),
        );

        let out: OutStream = Box::pin(crate::adapters::ssh::receiver_to_stream(evt_rx));
        Ok(Response::new(out))
    }

    async fn cleanup_job(
        &self,
        request: Request<tonic::Streaming<CleanupJobRequest>>,
    ) -> Result<Response<Self::CleanupJobStream>, Status> {
        let span = rpc_span(&request, "CleanupJob");
        let _enter = span.enter();
        tracing::info!(target: "orbitd::rpc", "received request");
        let remote_addr = format_remote_addr(request.remote_addr());
        let mut inbound = request.into_inner();
        let init = inbound
            .message()
            .await
            .map_err(|_| Status::unknown(codes::INTERNAL_ERROR))?
            .ok_or_else(|| Status::invalid_argument(codes::INVALID_ARGUMENT))?;

        let (job_id, force, full) = match init.msg {
            Some(proto::cleanup_job_request::Msg::Init(init)) => {
                (init.job_id, init.force, init.full)
            }
            _ => return Err(Status::invalid_argument(codes::INVALID_ARGUMENT)),
        };
        tracing::info!(
            "cleanup_job start remote_addr={remote_addr} job_id={job_id} force={force} full={full}"
        );
        let current_span = tracing::Span::current();

        let (evt_tx, evt_rx) = mpsc::channel::<Result<StreamEvent, Status>>(64);
        let (mfa_tx, mfa_rx) = mpsc::channel::<MfaAnswer>(16);
        tokio::spawn(
            async move {
                while let Ok(Some(item)) = inbound.message().await {
                    if let Some(proto::cleanup_job_request::Msg::Mfa(ans)) = item.msg
                        && mfa_tx.send(ans).await.is_err()
                    {
                        break;
                    }
                }
            }
            .instrument(current_span.clone()),
        );

        let input = CleanupJobInput {
            job_id,
            force,
            full,
        };
        let mut mfa_port = GrpcMfaPort { receiver: mfa_rx };
        let stream_output = GrpcStreamOutput {
            sender: evt_tx.clone(),
        };
        let usecases = self.usecases.clone();
        tokio::spawn(
            async move {
                if let Err(err) = usecases
                    .cleanup_job(input, &stream_output, &mut mfa_port)
                    .await
                {
                    let _ = evt_tx.send(Err(status_from_app_error(err))).await;
                }
            }
            .instrument(current_span.clone()),
        );

        let out: OutStream = Box::pin(crate::adapters::ssh::receiver_to_stream(evt_rx));
        Ok(Response::new(out))
    }

    async fn list_clusters(
        &self,
        request: Request<ListClustersRequest>,
    ) -> Result<Response<ListClustersResponse>, Status> {
        let span = rpc_span(&request, "ListClusters");
        let _enter = span.enter();
        tracing::info!(target: "orbitd::rpc", "received request");
        let remote_addr = format_remote_addr(request.remote_addr());
        let inbound = request.into_inner();
        let check_reachability = inbound.check_reachability.unwrap_or(true);
        let clusters = self
            .usecases
            .list_clusters(check_reachability)
            .await
            .map_err(status_from_app_error)?;
        let cluster_responses: Vec<_> = clusters.iter().map(cluster_status_to_response).collect();
        let connected_count = cluster_responses
            .iter()
            .filter(|cluster| cluster.connected)
            .count();
        tracing::info!(
            "list_clusters remote_addr={remote_addr} count={} connected={connected_count}",
            cluster_responses.len()
        );
        Ok(Response::new(ListClustersResponse {
            clusters: cluster_responses,
        }))
    }

    async fn delete_cluster(
        &self,
        request: Request<DeleteClusterRequest>,
    ) -> Result<Response<DeleteClusterResponse>, Status> {
        let span = rpc_span(&request, "DeleteCluster");
        let _enter = span.enter();
        tracing::info!(target: "orbitd::rpc", "received request");
        let remote_addr = format_remote_addr(request.remote_addr());
        let inbound = request.into_inner();
        let name = inbound.name.trim().to_string();
        let force = inbound.force;
        let deleted = self
            .usecases
            .delete_cluster(&name, force)
            .await
            .map_err(status_from_app_error)?;
        tracing::info!(
            "delete_cluster completed remote_addr={remote_addr} name={name} force={force}"
        );
        Ok(Response::new(DeleteClusterResponse { deleted }))
    }

    async fn list_partitions(
        &self,
        request: Request<ListPartitionsRequest>,
    ) -> Result<Response<ListPartitionsResponse>, Status> {
        let span = rpc_span(&request, "ListPartitions");
        let _enter = span.enter();
        tracing::info!(target: "orbitd::rpc", "received request");
        let remote_addr = format_remote_addr(request.remote_addr());
        let name = request.into_inner().name;
        let partitions = self
            .usecases
            .list_partitions(&name)
            .await
            .map_err(status_from_app_error)?;
        let responses = partitions
            .into_iter()
            .map(|name| ListPartitionsUnitResponse { name })
            .collect::<Vec<_>>();
        tracing::info!(
            "list_partitions remote_addr={remote_addr} name={name} count={}",
            responses.len()
        );
        Ok(Response::new(ListPartitionsResponse {
            partitions: responses,
        }))
    }

    async fn list_accounts(
        &self,
        request: Request<ListAccountsRequest>,
    ) -> Result<Response<ListAccountsResponse>, Status> {
        let span = rpc_span(&request, "ListAccounts");
        let _enter = span.enter();
        tracing::info!(target: "orbitd::rpc", "received request");
        let remote_addr = format_remote_addr(request.remote_addr());
        let name = request.into_inner().name;
        let accounts = self
            .usecases
            .list_accounts(&name)
            .await
            .map_err(status_from_app_error)?;
        let responses = accounts
            .into_iter()
            .map(|name| ListAccountsUnitResponse { name })
            .collect::<Vec<_>>();
        tracing::info!(
            "list_accounts remote_addr={remote_addr} name={name} count={}",
            responses.len()
        );
        Ok(Response::new(ListAccountsResponse {
            accounts: responses,
        }))
    }

    async fn run_job(
        &self,
        request: Request<tonic::Streaming<RunJobRequest>>,
    ) -> Result<Response<Self::RunJobStream>, Status> {
        let span = rpc_span(&request, "RunJob");
        let _enter = span.enter();
        tracing::info!(target: "orbitd::rpc", "received request");
        let remote_addr = format_remote_addr(request.remote_addr());
        let mut inbound = request.into_inner();
        let init = inbound
            .message()
            .await
            .map_err(|_| Status::unknown(codes::INTERNAL_ERROR))?
            .ok_or_else(|| Status::invalid_argument(codes::INVALID_ARGUMENT))?;

        let (
            local_path,
            remote_path,
            name,
            sbatchscript,
            filters,
            new_directory,
            blueprint_name,
            default_retrieve_path,
            template_values_json,
        ) = match init.msg {
            Some(proto::run_job_request::Msg::Init(init)) => (
                init.local_path,
                init.remote_path,
                init.name,
                init.sbatchscript,
                init.filters,
                init.new_directory,
                init.blueprint_name,
                init.default_retrieve_path,
                init.template_values_json,
            ),
            _ => return Err(Status::invalid_argument(codes::INVALID_ARGUMENT)),
        };
        tracing::info!(
            "run_job start remote_addr={remote_addr} name={name} local_path={local_path} requested_remote_path={:?} sbatch={sbatchscript}",
            remote_path
        );
        let current_span = tracing::Span::current();

        let filters = build_sync_filters(filters).map_err(status_from_app_error)?;

        let (evt_tx, evt_rx) = mpsc::channel::<Result<RunStreamEvent, Status>>(64);
        let (mfa_tx, mfa_rx) = mpsc::channel::<MfaAnswer>(16);
        let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);

        tokio::spawn(
            async move {
                while let Ok(Some(item)) = inbound.message().await {
                    if let Some(proto::run_job_request::Msg::Mfa(ans)) = item.msg
                        && mfa_tx.send(ans).await.is_err()
                    {
                        break;
                    }
                }
                let _ = cancel_tx.send(true);
            }
            .instrument(current_span.clone()),
        );

        let input = RunJobInput {
            local_path,
            remote_path,
            name,
            sbatchscript,
            filters,
            new_directory,
            blueprint_name,
            default_retrieve_path,
            template_values_json,
        };
        let mut mfa_port = GrpcMfaPort { receiver: mfa_rx };
        let stream_output = GrpcRunStreamOutput {
            sender: evt_tx.clone(),
        };
        let usecases = self.usecases.clone();
        tokio::spawn(
            async move {
                if let Err(err) = usecases
                    .run_job(input, &stream_output, &mut mfa_port, cancel_rx)
                    .await
                {
                    let _ = evt_tx.send(Err(status_from_app_error(err))).await;
                }
            }
            .instrument(current_span.clone()),
        );

        let out: RunOutStream = Box::pin(crate::adapters::ssh::receiver_to_stream(evt_rx));
        Ok(Response::new(out))
    }

    async fn run_blueprint(
        &self,
        request: Request<tonic::Streaming<RunBlueprintRequest>>,
    ) -> Result<Response<Self::RunBlueprintStream>, Status> {
        let span = rpc_span(&request, "RunBlueprint");
        let _enter = span.enter();
        tracing::info!(target: "orbitd::rpc", "received request");
        let remote_addr = format_remote_addr(request.remote_addr());
        let mut inbound = request.into_inner();
        let init = inbound
            .message()
            .await
            .map_err(|_| Status::unknown(codes::INTERNAL_ERROR))?
            .ok_or_else(|| Status::invalid_argument(codes::INVALID_ARGUMENT))?;

        let (
            blueprint_name,
            blueprint_tag,
            name,
            sbatchscript,
            filters,
            new_directory,
            remote_path,
            default_retrieve_path,
            template_values_json,
        ) = match init.msg {
            Some(proto::run_blueprint_request::Msg::Init(init)) => (
                init.blueprint_name,
                init.blueprint_tag,
                init.name,
                init.sbatchscript,
                init.filters,
                init.new_directory,
                init.remote_path,
                init.default_retrieve_path,
                init.template_values_json,
            ),
            _ => return Err(Status::invalid_argument(codes::INVALID_ARGUMENT)),
        };
        tracing::info!(
            "run_blueprint start remote_addr={remote_addr} name={name} blueprint={blueprint_name}:{blueprint_tag} requested_remote_path={:?} sbatch={sbatchscript}",
            remote_path
        );
        let current_span = tracing::Span::current();

        let filters = build_sync_filters(filters).map_err(status_from_app_error)?;

        let (evt_tx, evt_rx) = mpsc::channel::<Result<RunStreamEvent, Status>>(64);
        let (mfa_tx, mfa_rx) = mpsc::channel::<MfaAnswer>(16);
        let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);

        tokio::spawn(
            async move {
                while let Ok(Some(item)) = inbound.message().await {
                    if let Some(proto::run_blueprint_request::Msg::Mfa(ans)) = item.msg
                        && mfa_tx.send(ans).await.is_err()
                    {
                        break;
                    }
                }
                let _ = cancel_tx.send(true);
            }
            .instrument(current_span.clone()),
        );

        let input = RunBlueprintInput {
            blueprint_name,
            blueprint_tag,
            remote_path,
            name,
            sbatchscript,
            filters,
            new_directory,
            default_retrieve_path,
            template_values_json,
        };
        let mut mfa_port = GrpcMfaPort { receiver: mfa_rx };
        let stream_output = GrpcRunStreamOutput {
            sender: evt_tx.clone(),
        };
        let usecases = self.usecases.clone();
        tokio::spawn(
            async move {
                if let Err(err) = usecases
                    .run_blueprint(input, &stream_output, &mut mfa_port, cancel_rx)
                    .await
                {
                    let _ = evt_tx.send(Err(status_from_app_error(err))).await;
                }
            }
            .instrument(current_span.clone()),
        );

        let out: RunOutStream = Box::pin(crate::adapters::ssh::receiver_to_stream(evt_rx));
        Ok(Response::new(out))
    }

    async fn add_cluster(
        &self,
        request: Request<tonic::Streaming<AddClusterRequest>>,
    ) -> Result<Response<Self::AddClusterStream>, Status> {
        let span = rpc_span(&request, "AddCluster");
        let _enter = span.enter();
        tracing::info!(target: "orbitd::rpc", "received request");
        let remote_addr = format_remote_addr(request.remote_addr());
        let mut inbound = request.into_inner();
        let init = inbound
            .message()
            .await
            .map_err(|_| Status::unknown(codes::INTERNAL_ERROR))?
            .ok_or_else(|| Status::invalid_argument(codes::INVALID_ARGUMENT))?;

        let (
            username,
            host,
            name,
            identity_path,
            port,
            default_base_path,
            default_scratch_directory,
            interactive_scratch_selection,
        ) = match init.msg {
            Some(proto::add_cluster_request::Msg::Init(i)) => (
                i.username,
                i.host,
                i.name,
                i.identity_path,
                i.port,
                i.default_base_path,
                i.default_scratch_directory,
                i.interactive_scratch_selection,
            ),
            _ => {
                return Err(Status::invalid_argument(codes::INVALID_ARGUMENT));
            }
        };

        let address = parse_add_cluster_host(host)?;
        let port = parse_port(port)?;

        tracing::info!(
            "add_cluster start remote_addr={remote_addr} name={name} username={username} host={:?} port={port}",
            address
        );
        let current_span = tracing::Span::current();

        let (evt_tx, evt_rx) = mpsc::channel::<Result<StreamEvent, Status>>(64);
        let (mfa_tx, mfa_rx) = mpsc::channel::<MfaAnswer>(16);
        let (base_path_tx, mut base_path_rx) =
            mpsc::channel::<proto::AddClusterBasePathSelection>(16);
        let (scratch_tx, mut scratch_rx) = mpsc::channel::<proto::AddClusterScratchSelection>(16);
        let (default_tx, mut default_rx) = mpsc::channel::<proto::AddClusterDefaultSelection>(16);
        tokio::spawn(
            async move {
                while let Ok(Some(item)) = inbound.message().await {
                    match item.msg {
                        Some(proto::add_cluster_request::Msg::Mfa(ans)) => {
                            if mfa_tx.send(ans).await.is_err() {
                                break;
                            }
                        }
                        Some(proto::add_cluster_request::Msg::ScratchSelection(selection)) => {
                            if scratch_tx.send(selection).await.is_err() {
                                break;
                            }
                        }
                        Some(proto::add_cluster_request::Msg::BasePathSelection(selection)) => {
                            if base_path_tx.send(selection).await.is_err() {
                                break;
                            }
                        }
                        Some(proto::add_cluster_request::Msg::DefaultSelection(selection)) => {
                            if default_tx.send(selection).await.is_err() {
                                break;
                            }
                        }
                        _ => {}
                    }
                }
            }
            .instrument(current_span.clone()),
        );

        let mut mfa_port = GrpcMfaPort { receiver: mfa_rx };
        let stream_output = GrpcStreamOutput {
            sender: evt_tx.clone(),
        };
        let usecases = self.usecases.clone();
        tokio::spawn(
            async move {
                let mut resolved_default_base_path = default_base_path;
                let mut resolved_scratch_directory = default_scratch_directory;

                if interactive_scratch_selection && resolved_default_base_path.is_none() {
                    if stream_output
                        .send(StreamEvent {
                            event: Some(stream_event::Event::AddClusterBasePathPrompt(
                                proto::AddClusterBasePathPrompt {
                                    suggested_path: "~/runs".to_string(),
                                },
                            )),
                        })
                        .await
                        .is_err()
                    {
                        return;
                    }

                    loop {
                        let Some(selection) = base_path_rx.recv().await else {
                            let _ = evt_tx
                                .send(Err(Status::cancelled(codes::CANCELED)))
                                .await;
                            return;
                        };
                        let validate_input = ValidateDefaultBasePathInput {
                            username: username.clone(),
                            address: address.clone(),
                            port,
                            identity_path: identity_path.clone(),
                            session_name: Some(name.clone()),
                            base_path: selection.path,
                        };
                        match usecases
                            .validate_default_base_path(validate_input, &stream_output, &mut mfa_port)
                            .await
                        {
                            Ok(path) => {
                                resolved_default_base_path = Some(path.clone());
                                if stream_output
                                    .send(StreamEvent {
                                        event: Some(
                                            stream_event::Event::AddClusterBasePathValidation(
                                                proto::AddClusterBasePathValidation {
                                                    accepted: true,
                                                    path: Some(path),
                                                    error: None,
                                                },
                                            ),
                                        ),
                                    })
                                    .await
                                    .is_err()
                                {
                                    return;
                                }
                                break;
                            }
                            Err(err) if err.kind() == AppErrorKind::InvalidArgument => {
                                if stream_output
                                    .send(StreamEvent {
                                        event: Some(
                                            stream_event::Event::AddClusterBasePathValidation(
                                                proto::AddClusterBasePathValidation {
                                                    accepted: false,
                                                    path: None,
                                                    error: Some(err.message().to_string()),
                                                },
                                            ),
                                        ),
                                    })
                                    .await
                                    .is_err()
                                {
                                    return;
                                }
                            }
                            Err(err) => {
                                let _ = evt_tx.send(Err(status_from_app_error(err))).await;
                                return;
                            }
                        }
                    }
                }

                if interactive_scratch_selection && resolved_scratch_directory.is_none() {
                    let resolve_input = ResolveScratchDirectoriesInput {
                        username: username.clone(),
                        address: address.clone(),
                        port,
                        identity_path: identity_path.clone(),
                        session_name: Some(name.clone()),
                    };
                    let candidates = match usecases
                        .resolve_scratch_directories(resolve_input, &stream_output, &mut mfa_port)
                        .await
                    {
                        Ok(found) => found,
                        Err(err) => {
                            let _ = evt_tx.send(Err(status_from_app_error(err))).await;
                            return;
                        }
                    };

                    if stream_output
                        .send(StreamEvent {
                            event: Some(stream_event::Event::AddClusterScratchOptions(
                                proto::AddClusterScratchOptions {
                                    directories: candidates,
                                },
                            )),
                        })
                        .await
                        .is_err()
                    {
                        return;
                    }

                    loop {
                        let Some(selection) = scratch_rx.recv().await else {
                            let _ = evt_tx
                                .send(Err(Status::cancelled(codes::CANCELED)))
                                .await;
                            return;
                        };

                        let selected = match selection.selection {
                            Some(proto::add_cluster_scratch_selection::Selection::Directory(
                                directory,
                            )) => Some(directory),
                            Some(proto::add_cluster_scratch_selection::Selection::None(_)) => None,
                            None => {
                                if stream_output
                                    .send(StreamEvent {
                                        event: Some(stream_event::Event::AddClusterScratchValidation(
                                            proto::AddClusterScratchValidation {
                                                accepted: false,
                                                directory: None,
                                                error: Some(
                                                    "scratch directory selection is required"
                                                        .to_string(),
                                                ),
                                            },
                                        )),
                                    })
                                    .await
                                    .is_err()
                                {
                                    return;
                                }
                                continue;
                            }
                        };

                        match selected {
                            None => {
                                resolved_scratch_directory = None;
                                if stream_output
                                    .send(StreamEvent {
                                        event: Some(stream_event::Event::AddClusterScratchValidation(
                                            proto::AddClusterScratchValidation {
                                                accepted: true,
                                                directory: None,
                                                error: None,
                                            },
                                        )),
                                    })
                                    .await
                                    .is_err()
                                {
                                    return;
                                }
                                break;
                            }
                            Some(directory) => {
                                let validate_input = ValidateScratchDirectoryInput {
                                    username: username.clone(),
                                    address: address.clone(),
                                    port,
                                    identity_path: identity_path.clone(),
                                    session_name: Some(name.clone()),
                                    directory,
                                };
                                match usecases
                                    .validate_scratch_directory(
                                        validate_input,
                                        &stream_output,
                                        &mut mfa_port,
                                    )
                                    .await
                                {
                                    Ok(path) => {
                                        resolved_scratch_directory = Some(path.clone());
                                        if stream_output
                                            .send(StreamEvent {
                                                event: Some(
                                                    stream_event::Event::AddClusterScratchValidation(
                                                        proto::AddClusterScratchValidation {
                                                            accepted: true,
                                                            directory: Some(path),
                                                            error: None,
                                                        },
                                                    ),
                                                ),
                                            })
                                            .await
                                            .is_err()
                                        {
                                            return;
                                        }
                                        break;
                                    }
                                    Err(err) if err.kind() == AppErrorKind::InvalidArgument => {
                                        if stream_output
                                            .send(StreamEvent {
                                                event: Some(
                                                    stream_event::Event::AddClusterScratchValidation(
                                                        proto::AddClusterScratchValidation {
                                                            accepted: false,
                                                            directory: None,
                                                            error: Some(
                                                                err.message().to_string(),
                                                            ),
                                                        },
                                                    ),
                                                ),
                                            })
                                            .await
                                            .is_err()
                                        {
                                            return;
                                        }
                                    }
                                    Err(err) => {
                                        let _ = evt_tx.send(Err(status_from_app_error(err))).await;
                                        return;
                                    }
                                }
                            }
                        }
                    }
                }

                let input = AddClusterInput {
                    name,
                    username,
                    address,
                    port,
                    identity_path,
                    default_base_path: resolved_default_base_path,
                    default_scratch_directory: resolved_scratch_directory,
                };
                if let Err(err) = usecases
                    .add_cluster(input, &stream_output, &mut mfa_port, &mut default_rx)
                    .await
                {
                    let _ = evt_tx.send(Err(status_from_app_error(err))).await;
                }
            }
            .instrument(current_span.clone()),
        );

        let out: OutStream = Box::pin(crate::adapters::ssh::receiver_to_stream(evt_rx));
        Ok(Response::new(out))
    }

    async fn set_cluster(
        &self,
        request: Request<tonic::Streaming<SetClusterRequest>>,
    ) -> Result<Response<Self::SetClusterStream>, Status> {
        let span = rpc_span(&request, "SetCluster");
        let _enter = span.enter();
        tracing::info!(target: "orbitd::rpc", "received request");
        let remote_addr = format_remote_addr(request.remote_addr());
        let mut inbound = request.into_inner();
        let init = inbound
            .message()
            .await
            .map_err(|_| Status::unknown(codes::INTERNAL_ERROR))?
            .ok_or_else(|| Status::invalid_argument(codes::INVALID_ARGUMENT))?;

        let (name, host, username, port, identity_path, default_base_path, is_default) =
            match init.msg {
                Some(proto::set_cluster_request::Msg::Init(i)) => (
                    i.name,
                    i.host,
                    i.username,
                    i.port,
                    i.identity_path,
                    i.default_base_path,
                    i.is_default,
                ),
                _ => {
                    return Err(Status::invalid_argument(codes::INVALID_ARGUMENT));
                }
            };
        tracing::info!(
            "set_cluster start remote_addr={remote_addr} name={name} host={host:?} username={username:?}"
        );
        let current_span = tracing::Span::current();

        let address = match host {
            Some(value) => Some(parse_set_cluster_host(&value)?),
            None => None,
        };
        let port = match port {
            Some(value) => Some(parse_port(value)?),
            None => None,
        };

        let (evt_tx, evt_rx) = mpsc::channel::<Result<StreamEvent, Status>>(64);
        let (mfa_tx, mfa_rx) = mpsc::channel::<MfaAnswer>(16);
        tokio::spawn(
            async move {
                while let Ok(Some(item)) = inbound.message().await {
                    if let Some(proto::set_cluster_request::Msg::Mfa(ans)) = item.msg
                        && mfa_tx.send(ans).await.is_err()
                    {
                        break;
                    }
                }
            }
            .instrument(current_span.clone()),
        );

        let input = SetClusterInput {
            name,
            address,
            username,
            port,
            identity_path,
            default_base_path,
            is_default,
        };
        let mut mfa_port = GrpcMfaPort { receiver: mfa_rx };
        let stream_output = GrpcStreamOutput {
            sender: evt_tx.clone(),
        };
        let usecases = self.usecases.clone();
        tokio::spawn(
            async move {
                if let Err(err) = usecases
                    .set_cluster(input, &stream_output, &mut mfa_port)
                    .await
                {
                    let _ = evt_tx.send(Err(status_from_app_error(err))).await;
                }
            }
            .instrument(current_span.clone()),
        );

        let out: OutStream = Box::pin(crate::adapters::ssh::receiver_to_stream(evt_rx));
        Ok(Response::new(out))
    }

    async fn reconnect_cluster(
        &self,
        request: Request<tonic::Streaming<ReconnectClusterRequest>>,
    ) -> Result<Response<Self::ReconnectClusterStream>, Status> {
        let span = rpc_span(&request, "ReconnectCluster");
        let _enter = span.enter();
        tracing::info!(target: "orbitd::rpc", "received request");
        let remote_addr = format_remote_addr(request.remote_addr());
        let mut inbound = request.into_inner();
        let init = inbound
            .message()
            .await
            .map_err(|_| Status::unknown(codes::INTERNAL_ERROR))?
            .ok_or_else(|| Status::invalid_argument(codes::INVALID_ARGUMENT))?;

        let name = match init.msg {
            Some(proto::reconnect_cluster_request::Msg::Init(i)) => i.name,
            _ => {
                return Err(Status::invalid_argument(codes::INVALID_ARGUMENT));
            }
        };
        tracing::info!("reconnect_cluster start remote_addr={remote_addr} name={name}");
        let current_span = tracing::Span::current();

        let (evt_tx, evt_rx) = mpsc::channel::<Result<ReconnectClusterStreamEvent, Status>>(64);
        let (mfa_tx, mfa_rx) = mpsc::channel::<MfaAnswer>(16);
        tokio::spawn(
            async move {
                while let Ok(Some(item)) = inbound.message().await {
                    if let Some(proto::reconnect_cluster_request::Msg::Mfa(ans)) = item.msg
                        && mfa_tx.send(ans).await.is_err()
                    {
                        break;
                    }
                }
            }
            .instrument(current_span.clone()),
        );

        let mut mfa_port = GrpcMfaPort { receiver: mfa_rx };
        let stream_output = GrpcReconnectClusterStreamOutput {
            sender: evt_tx.clone(),
        };
        let usecases = self.usecases.clone();
        tokio::spawn(
            async move {
                if let Err(err) = usecases
                    .reconnect_cluster(&name, &stream_output, &mut mfa_port)
                    .await
                {
                    let _ = evt_tx.send(Err(status_from_app_error(err))).await;
                    return;
                }

                let _ = evt_tx
                    .send(Ok(ReconnectClusterStreamEvent {
                        event: Some(reconnect_cluster_stream_event::Event::Done(
                            proto::ReconnectClusterDone {},
                        )),
                    }))
                    .await;
            }
            .instrument(current_span.clone()),
        );

        let out: ReconnectClusterOutStream =
            Box::pin(crate::adapters::ssh::receiver_to_stream(evt_rx));
        Ok(Response::new(out))
    }

    async fn list_jobs(
        &self,
        request: Request<ListJobsRequest>,
    ) -> Result<Response<ListJobsResponse>, Status> {
        let span = rpc_span(&request, "ListJobs");
        let _enter = span.enter();
        tracing::info!(target: "orbitd::rpc", "received request");
        let remote_addr = format_remote_addr(request.remote_addr());
        let inbound = request.into_inner();
        let name_filter = inbound.name.as_deref();
        let blueprint_filter = inbound.blueprint_name.as_deref();

        let jobs = self
            .usecases
            .list_jobs(name_filter, blueprint_filter)
            .await
            .map_err(status_from_app_error)?;
        let api_jobs: Vec<_> = jobs.iter().map(job_record_to_response).collect();
        let name_label = name_filter.unwrap_or("<all>");
        let blueprint_label = blueprint_filter.unwrap_or("<all>");
        tracing::info!(
            "list_jobs remote_addr={remote_addr} name={name_label} blueprint={blueprint_label} count={}",
            api_jobs.len()
        );
        Ok(Response::new(ListJobsResponse { jobs: api_jobs }))
    }

    async fn upsert_blueprint(
        &self,
        request: Request<UpsertBlueprintRequest>,
    ) -> Result<Response<UpsertBlueprintResponse>, Status> {
        let span = rpc_span(&request, "UpsertBlueprint");
        let _enter = span.enter();
        tracing::info!(target: "orbitd::rpc", "received request");
        let remote_addr = format_remote_addr(request.remote_addr());
        let req = request.into_inner();
        let blueprint = self
            .usecases
            .upsert_blueprint(&req.name)
            .await
            .map_err(status_from_app_error)?;
        tracing::info!(
            "upsert_blueprint remote_addr={remote_addr} name={}",
            blueprint.name
        );
        Ok(Response::new(UpsertBlueprintResponse {
            blueprint: Some(blueprint_record_to_response(&blueprint)),
        }))
    }

    async fn get_blueprint(
        &self,
        request: Request<GetBlueprintRequest>,
    ) -> Result<Response<GetBlueprintResponse>, Status> {
        let span = rpc_span(&request, "GetBlueprint");
        let _enter = span.enter();
        tracing::info!(target: "orbitd::rpc", "received request");
        let remote_addr = format_remote_addr(request.remote_addr());
        let req = request.into_inner();
        let blueprint = self
            .usecases
            .get_blueprint_by_name(&req.name)
            .await
            .map_err(status_from_app_error)?;
        tracing::info!(
            "get_blueprint remote_addr={remote_addr} name={}",
            blueprint.name
        );
        Ok(Response::new(GetBlueprintResponse {
            blueprint: Some(blueprint_record_to_response(&blueprint)),
        }))
    }

    async fn list_blueprints(
        &self,
        request: Request<ListBlueprintsRequest>,
    ) -> Result<Response<ListBlueprintsResponse>, Status> {
        let span = rpc_span(&request, "ListBlueprints");
        let _enter = span.enter();
        tracing::info!(target: "orbitd::rpc", "received request");
        let remote_addr = format_remote_addr(request.remote_addr());
        let _ = request.into_inner();
        let blueprints = self
            .usecases
            .list_blueprints()
            .await
            .map_err(status_from_app_error)?;
        let blueprints = blueprints
            .iter()
            .map(blueprint_record_to_response)
            .collect::<Vec<_>>();
        tracing::info!(
            "list_blueprints remote_addr={remote_addr} count={}",
            blueprints.len()
        );
        Ok(Response::new(ListBlueprintsResponse { blueprints }))
    }

    async fn delete_blueprint(
        &self,
        request: Request<DeleteBlueprintRequest>,
    ) -> Result<Response<DeleteBlueprintResponse>, Status> {
        let span = rpc_span(&request, "DeleteBlueprint");
        let _enter = span.enter();
        tracing::info!(target: "orbitd::rpc", "received request");
        let remote_addr = format_remote_addr(request.remote_addr());
        let name = request.into_inner().name.trim().to_string();
        let deleted = self
            .usecases
            .delete_blueprint(&name)
            .await
            .map_err(status_from_app_error)?;
        tracing::info!("delete_blueprint remote_addr={remote_addr} name={name} deleted={deleted}");
        Ok(Response::new(DeleteBlueprintResponse { deleted }))
    }

    async fn build_blueprint(
        &self,
        request: Request<BuildBlueprintRequest>,
    ) -> Result<Response<BuildBlueprintResponse>, Status> {
        let span = rpc_span(&request, "BuildBlueprint");
        let _enter = span.enter();
        tracing::info!(target: "orbitd::rpc", "received request");
        let remote_addr = format_remote_addr(request.remote_addr());
        let req = request.into_inner();
        let blueprint = self
            .usecases
            .build_blueprint(&req.path, req.package_git)
            .await
            .map_err(status_from_app_error)?;
        tracing::info!(
            "build_blueprint remote_addr={remote_addr} name={}",
            blueprint.name
        );
        Ok(Response::new(BuildBlueprintResponse {
            blueprint: Some(blueprint_record_to_response(&blueprint)),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proto::add_cluster_init;
    use tonic::Code;

    #[test]
    fn parse_add_cluster_host_validates() {
        assert_eq!(
            parse_add_cluster_host(None).unwrap_err().message(),
            codes::INVALID_ARGUMENT
        );

        let host = parse_add_cluster_host(Some(add_cluster_init::Host::Hostname(
            "example.com".to_string(),
        )))
        .unwrap();
        assert!(matches!(host, Address::Hostname(_)));

        let host = parse_add_cluster_host(Some(add_cluster_init::Host::Ipaddr(
            "127.0.0.1".to_string(),
        )))
        .unwrap();
        assert!(matches!(host, Address::Ip(_)));

        let err = parse_add_cluster_host(Some(add_cluster_init::Host::Ipaddr(
            "not-an-ip".to_string(),
        )))
        .unwrap_err();
        assert_eq!(err.message(), codes::INVALID_ARGUMENT);
    }

    #[test]
    fn parse_port_validates() {
        assert_eq!(parse_port(22).unwrap(), 22u16);
        let err = parse_port(u32::from(u16::MAX) + 1).unwrap_err();
        assert_eq!(err.message(), codes::INVALID_ARGUMENT);
    }

    #[test]
    fn status_from_app_error_maps_not_found_with_message() {
        let status = status_from_app_error(AppError::with_message(
            AppErrorKind::NotFound,
            codes::NOT_FOUND,
            "cluster 'missing' not found",
        ));
        assert_eq!(status.code(), Code::NotFound);
        assert_eq!(status.message(), "cluster 'missing' not found");
    }
}
