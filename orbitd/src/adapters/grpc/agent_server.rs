// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;

use async_trait::async_trait;
use proto::agent_server::Agent;
use proto::{
    AddClusterRequest, CancelJobRequest, CleanupJobRequest, DeleteClusterRequest,
    DeleteClusterResponse, DeleteProjectRequest, DeleteProjectResponse, GetProjectRequest,
    GetProjectResponse, JobLogsRequest, ListClustersRequest, ListClustersResponse, ListJobsRequest,
    ListJobsResponse, ListProjectsRequest, ListProjectsResponse, LsRequest, MfaAnswer, PingReply,
    PingRequest, ResolveHomeDirRequest, RetrieveJobRequest, SetClusterRequest, StreamEvent,
    SubmitRequest, SubmitStreamEvent, UpsertProjectRequest, UpsertProjectResponse,
};
use tokio::sync::mpsc;
use tokio_stream::Stream;
use tonic::{Request, Response, Status};

use crate::adapters::grpc::mapping::{
    build_sync_filters, cluster_status_to_response, job_record_to_response,
    project_record_to_response,
};
use crate::app::errors::{AppError, AppErrorKind, codes};
use crate::app::ports::{MfaPort, StreamOutputPort, SubmitStreamOutputPort};
use crate::app::types::Address;
use crate::app::usecases::{
    AddClusterInput, CancelJobInput, CleanupJobInput, JobLogsInput, LsInput, ResolveHomeDirInput,
    RetrieveJobInput, SetClusterInput, SubmitInput, UseCases,
};

pub type OutStream =
    Pin<Box<dyn Stream<Item = Result<StreamEvent, Status>> + Send + Sync + 'static>>;
pub type SubmitOutStream =
    Pin<Box<dyn Stream<Item = Result<SubmitStreamEvent, Status>> + Send + Sync + 'static>>;

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
struct GrpcSubmitStreamOutput {
    sender: mpsc::Sender<Result<SubmitStreamEvent, Status>>,
}

#[async_trait]
impl SubmitStreamOutputPort for GrpcSubmitStreamOutput {
    async fn send(&self, event: SubmitStreamEvent) -> Result<(), AppError> {
        self.sender
            .send(Ok(event))
            .await
            .map_err(|_| AppError::new(AppErrorKind::Cancelled, codes::CANCELED))
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
    type SubmitStream = SubmitOutStream;
    type AddClusterStream = OutStream;
    type SetClusterStream = OutStream;
    type ResolveHomeDirStream = OutStream;

    async fn ping(&self, request: Request<PingRequest>) -> Result<Response<PingReply>, Status> {
        let remote_addr = format_remote_addr(request.remote_addr());
        let req = request.into_inner();
        match self.usecases.ping(&req.message).await {
            Ok(message) => {
                log::info!("ping remote_addr={remote_addr}");
                Ok(Response::new(PingReply { message }))
            }
            Err(err) => {
                log::warn!(
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
        log::info!(
            "resolve_home_dir start remote_addr={remote_addr} session_name={} username={} host={:?} port={port}",
            session_name.as_deref().unwrap_or("<none>"),
            username,
            address
        );

        let (evt_tx, evt_rx) = mpsc::channel::<Result<StreamEvent, Status>>(64);
        let (mfa_tx, mfa_rx) = mpsc::channel::<MfaAnswer>(16);
        tokio::spawn(async move {
            while let Ok(Some(item)) = inbound.message().await {
                if let Some(proto::resolve_home_dir_request::Msg::Mfa(ans)) = item.msg
                    && mfa_tx.send(ans).await.is_err()
                {
                    break;
                }
            }
        });

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
        tokio::spawn(async move {
            if let Err(err) = usecases
                .resolve_home_dir(input, &stream_output, &mut mfa_port)
                .await
            {
                let _ = evt_tx.send(Err(status_from_app_error(err))).await;
            }
        });

        let out: OutStream = Box::pin(crate::adapters::ssh::receiver_to_stream(evt_rx));
        Ok(Response::new(out))
    }

    async fn ls(
        &self,
        request: Request<tonic::Streaming<LsRequest>>,
    ) -> Result<Response<Self::LsStream>, Status> {
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
        log::info!(
            "ls start remote_addr={remote_addr} name={} job_id={:?} requested_path={:?}",
            name,
            job_id,
            path
        );

        let (evt_tx, evt_rx) = mpsc::channel::<Result<StreamEvent, Status>>(64);
        let (mfa_tx, mfa_rx) = mpsc::channel::<MfaAnswer>(16);
        tokio::spawn(async move {
            while let Ok(Some(item)) = inbound.message().await {
                if let Some(proto::ls_request::Msg::Mfa(ans)) = item.msg
                    && mfa_tx.send(ans).await.is_err()
                {
                    break;
                }
            }
        });

        let input = LsInput { name, path, job_id };
        let mut mfa_port = GrpcMfaPort { receiver: mfa_rx };
        let stream_output = GrpcStreamOutput {
            sender: evt_tx.clone(),
        };
        let usecases = self.usecases.clone();
        tokio::spawn(async move {
            if let Err(err) = usecases.ls(input, &stream_output, &mut mfa_port).await {
                let _ = evt_tx.send(Err(status_from_app_error(err))).await;
            }
        });

        let out: OutStream = Box::pin(crate::adapters::ssh::receiver_to_stream(evt_rx));
        Ok(Response::new(out))
    }

    async fn retrieve_job(
        &self,
        request: Request<tonic::Streaming<RetrieveJobRequest>>,
    ) -> Result<Response<Self::RetrieveJobStream>, Status> {
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
        log::info!(
            "retrieve_job start remote_addr={remote_addr} job_id={job_id} path={path} local_path={:?}",
            local_path
        );

        let (evt_tx, evt_rx) = mpsc::channel::<Result<StreamEvent, Status>>(64);
        let (mfa_tx, mfa_rx) = mpsc::channel::<MfaAnswer>(16);
        tokio::spawn(async move {
            while let Ok(Some(item)) = inbound.message().await {
                if let Some(proto::retrieve_job_request::Msg::Mfa(ans)) = item.msg
                    && mfa_tx.send(ans).await.is_err()
                {
                    break;
                }
            }
        });

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
        tokio::spawn(async move {
            if let Err(err) = usecases
                .retrieve_job(input, &stream_output, &mut mfa_port)
                .await
            {
                let _ = evt_tx.send(Err(status_from_app_error(err))).await;
            }
        });

        let out: OutStream = Box::pin(crate::adapters::ssh::receiver_to_stream(evt_rx));
        Ok(Response::new(out))
    }

    async fn job_logs(
        &self,
        request: Request<tonic::Streaming<JobLogsRequest>>,
    ) -> Result<Response<Self::JobLogsStream>, Status> {
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
        log::info!("job_logs start remote_addr={remote_addr} job_id={job_id} stderr={stderr}");

        let (evt_tx, evt_rx) = mpsc::channel::<Result<StreamEvent, Status>>(64);
        let (mfa_tx, mfa_rx) = mpsc::channel::<MfaAnswer>(16);
        tokio::spawn(async move {
            while let Ok(Some(item)) = inbound.message().await {
                if let Some(proto::job_logs_request::Msg::Mfa(ans)) = item.msg
                    && mfa_tx.send(ans).await.is_err()
                {
                    break;
                }
            }
        });

        let input = JobLogsInput { job_id, stderr };
        let mut mfa_port = GrpcMfaPort { receiver: mfa_rx };
        let stream_output = GrpcStreamOutput {
            sender: evt_tx.clone(),
        };
        let usecases = self.usecases.clone();
        tokio::spawn(async move {
            if let Err(err) = usecases
                .job_logs(input, &stream_output, &mut mfa_port)
                .await
            {
                let _ = evt_tx.send(Err(status_from_app_error(err))).await;
            }
        });

        let out: OutStream = Box::pin(crate::adapters::ssh::receiver_to_stream(evt_rx));
        Ok(Response::new(out))
    }

    async fn cancel_job(
        &self,
        request: Request<tonic::Streaming<CancelJobRequest>>,
    ) -> Result<Response<Self::CancelJobStream>, Status> {
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
        log::info!("cancel_job start remote_addr={remote_addr} job_id={job_id}");

        let (evt_tx, evt_rx) = mpsc::channel::<Result<StreamEvent, Status>>(64);
        let (mfa_tx, mfa_rx) = mpsc::channel::<MfaAnswer>(16);
        tokio::spawn(async move {
            while let Ok(Some(item)) = inbound.message().await {
                if let Some(proto::cancel_job_request::Msg::Mfa(ans)) = item.msg
                    && mfa_tx.send(ans).await.is_err()
                {
                    break;
                }
            }
        });

        let input = CancelJobInput { job_id };
        let mut mfa_port = GrpcMfaPort { receiver: mfa_rx };
        let stream_output = GrpcStreamOutput {
            sender: evt_tx.clone(),
        };
        let usecases = self.usecases.clone();
        tokio::spawn(async move {
            if let Err(err) = usecases
                .cancel_job(input, &stream_output, &mut mfa_port)
                .await
            {
                let _ = evt_tx.send(Err(status_from_app_error(err))).await;
            }
        });

        let out: OutStream = Box::pin(crate::adapters::ssh::receiver_to_stream(evt_rx));
        Ok(Response::new(out))
    }

    async fn cleanup_job(
        &self,
        request: Request<tonic::Streaming<CleanupJobRequest>>,
    ) -> Result<Response<Self::CleanupJobStream>, Status> {
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
        log::info!(
            "cleanup_job start remote_addr={remote_addr} job_id={job_id} force={force} full={full}"
        );

        let (evt_tx, evt_rx) = mpsc::channel::<Result<StreamEvent, Status>>(64);
        let (mfa_tx, mfa_rx) = mpsc::channel::<MfaAnswer>(16);
        tokio::spawn(async move {
            while let Ok(Some(item)) = inbound.message().await {
                if let Some(proto::cleanup_job_request::Msg::Mfa(ans)) = item.msg
                    && mfa_tx.send(ans).await.is_err()
                {
                    break;
                }
            }
        });

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
        tokio::spawn(async move {
            if let Err(err) = usecases
                .cleanup_job(input, &stream_output, &mut mfa_port)
                .await
            {
                let _ = evt_tx.send(Err(status_from_app_error(err))).await;
            }
        });

        let out: OutStream = Box::pin(crate::adapters::ssh::receiver_to_stream(evt_rx));
        Ok(Response::new(out))
    }

    async fn list_clusters(
        &self,
        request: Request<ListClustersRequest>,
    ) -> Result<Response<ListClustersResponse>, Status> {
        let remote_addr = format_remote_addr(request.remote_addr());
        let _inbound = request.into_inner();
        let clusters = self
            .usecases
            .list_clusters()
            .await
            .map_err(status_from_app_error)?;
        let cluster_responses: Vec<_> = clusters.iter().map(cluster_status_to_response).collect();
        let connected_count = cluster_responses
            .iter()
            .filter(|cluster| cluster.connected)
            .count();
        log::info!(
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
        let remote_addr = format_remote_addr(request.remote_addr());
        let name = request.into_inner().name.trim().to_string();
        let deleted = self
            .usecases
            .delete_cluster(&name)
            .await
            .map_err(status_from_app_error)?;
        log::info!("delete_cluster completed remote_addr={remote_addr} name={name}");
        Ok(Response::new(DeleteClusterResponse { deleted }))
    }

    async fn submit(
        &self,
        request: Request<tonic::Streaming<SubmitRequest>>,
    ) -> Result<Response<Self::SubmitStream>, Status> {
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
            force,
            project_name,
            default_retrieve_path,
            template_values_json,
        ) = match init.msg {
            Some(proto::submit_request::Msg::Init(init)) => (
                init.local_path,
                init.remote_path,
                init.name,
                init.sbatchscript,
                init.filters,
                init.new_directory,
                init.force,
                init.project_name,
                init.default_retrieve_path,
                init.template_values_json,
            ),
            _ => return Err(Status::invalid_argument(codes::INVALID_ARGUMENT)),
        };
        log::info!(
            "submit start remote_addr={remote_addr} name={name} local_path={local_path} requested_remote_path={:?} sbatch={sbatchscript}",
            remote_path
        );

        let filters = build_sync_filters(filters).map_err(status_from_app_error)?;

        let (evt_tx, evt_rx) = mpsc::channel::<Result<SubmitStreamEvent, Status>>(64);
        let (mfa_tx, mfa_rx) = mpsc::channel::<MfaAnswer>(16);
        let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);

        tokio::spawn(async move {
            while let Ok(Some(item)) = inbound.message().await {
                if let Some(proto::submit_request::Msg::Mfa(ans)) = item.msg
                    && mfa_tx.send(ans).await.is_err()
                {
                    break;
                }
            }
            let _ = cancel_tx.send(true);
        });

        let input = SubmitInput {
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
        };
        let mut mfa_port = GrpcMfaPort { receiver: mfa_rx };
        let stream_output = GrpcSubmitStreamOutput {
            sender: evt_tx.clone(),
        };
        let usecases = self.usecases.clone();
        tokio::spawn(async move {
            if let Err(err) = usecases
                .submit(input, &stream_output, &mut mfa_port, cancel_rx)
                .await
            {
                let _ = evt_tx.send(Err(status_from_app_error(err))).await;
            }
        });

        let out: SubmitOutStream = Box::pin(crate::adapters::ssh::receiver_to_stream(evt_rx));
        Ok(Response::new(out))
    }

    async fn add_cluster(
        &self,
        request: Request<tonic::Streaming<AddClusterRequest>>,
    ) -> Result<Response<Self::AddClusterStream>, Status> {
        let remote_addr = format_remote_addr(request.remote_addr());
        let mut inbound = request.into_inner();
        let init = inbound
            .message()
            .await
            .map_err(|_| Status::unknown(codes::INTERNAL_ERROR))?
            .ok_or_else(|| Status::invalid_argument(codes::INVALID_ARGUMENT))?;

        let (username, host, name, identity_path, port, default_base_path) = match init.msg {
            Some(proto::add_cluster_request::Msg::Init(i)) => (
                i.username,
                i.host,
                i.name,
                i.identity_path,
                i.port,
                i.default_base_path,
            ),
            _ => {
                return Err(Status::invalid_argument(codes::INVALID_ARGUMENT));
            }
        };

        let address = parse_add_cluster_host(host)?;
        let port = parse_port(port)?;

        log::info!(
            "add_cluster start remote_addr={remote_addr} name={name} username={username} host={:?} port={port}",
            address
        );

        let (evt_tx, evt_rx) = mpsc::channel::<Result<StreamEvent, Status>>(64);
        let (mfa_tx, mfa_rx) = mpsc::channel::<MfaAnswer>(16);
        tokio::spawn(async move {
            while let Ok(Some(item)) = inbound.message().await {
                if let Some(proto::add_cluster_request::Msg::Mfa(ans)) = item.msg
                    && mfa_tx.send(ans).await.is_err()
                {
                    break;
                }
            }
        });

        let input = AddClusterInput {
            name,
            username,
            address,
            port,
            identity_path,
            default_base_path,
        };
        let mut mfa_port = GrpcMfaPort { receiver: mfa_rx };
        let stream_output = GrpcStreamOutput {
            sender: evt_tx.clone(),
        };
        let usecases = self.usecases.clone();
        tokio::spawn(async move {
            if let Err(err) = usecases
                .add_cluster(input, &stream_output, &mut mfa_port)
                .await
            {
                let _ = evt_tx.send(Err(status_from_app_error(err))).await;
            }
        });

        let out: OutStream = Box::pin(crate::adapters::ssh::receiver_to_stream(evt_rx));
        Ok(Response::new(out))
    }

    async fn set_cluster(
        &self,
        request: Request<tonic::Streaming<SetClusterRequest>>,
    ) -> Result<Response<Self::SetClusterStream>, Status> {
        let remote_addr = format_remote_addr(request.remote_addr());
        let mut inbound = request.into_inner();
        let init = inbound
            .message()
            .await
            .map_err(|_| Status::unknown(codes::INTERNAL_ERROR))?
            .ok_or_else(|| Status::invalid_argument(codes::INVALID_ARGUMENT))?;

        let (name, host, username, port, identity_path, default_base_path) = match init.msg {
            Some(proto::set_cluster_request::Msg::Init(i)) => (
                i.name,
                i.host,
                i.username,
                i.port,
                i.identity_path,
                i.default_base_path,
            ),
            _ => {
                return Err(Status::invalid_argument(codes::INVALID_ARGUMENT));
            }
        };
        log::info!(
            "set_cluster start remote_addr={remote_addr} name={name} host={host:?} username={username:?}"
        );

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
        tokio::spawn(async move {
            while let Ok(Some(item)) = inbound.message().await {
                if let Some(proto::set_cluster_request::Msg::Mfa(ans)) = item.msg
                    && mfa_tx.send(ans).await.is_err()
                {
                    break;
                }
            }
        });

        let input = SetClusterInput {
            name,
            address,
            username,
            port,
            identity_path,
            default_base_path,
        };
        let mut mfa_port = GrpcMfaPort { receiver: mfa_rx };
        let stream_output = GrpcStreamOutput {
            sender: evt_tx.clone(),
        };
        let usecases = self.usecases.clone();
        tokio::spawn(async move {
            if let Err(err) = usecases
                .set_cluster(input, &stream_output, &mut mfa_port)
                .await
            {
                let _ = evt_tx.send(Err(status_from_app_error(err))).await;
            }
        });

        let out: OutStream = Box::pin(crate::adapters::ssh::receiver_to_stream(evt_rx));
        Ok(Response::new(out))
    }

    async fn list_jobs(
        &self,
        request: Request<ListJobsRequest>,
    ) -> Result<Response<ListJobsResponse>, Status> {
        let remote_addr = format_remote_addr(request.remote_addr());
        let inbound = request.into_inner();
        let name_filter = inbound.name.as_deref();

        let jobs = self
            .usecases
            .list_jobs(name_filter)
            .await
            .map_err(status_from_app_error)?;
        let api_jobs: Vec<_> = jobs.iter().map(job_record_to_response).collect();
        let name_label = name_filter.unwrap_or("<all>");
        log::info!(
            "list_jobs remote_addr={remote_addr} name={name_label} count={}",
            api_jobs.len()
        );
        Ok(Response::new(ListJobsResponse { jobs: api_jobs }))
    }

    async fn upsert_project(
        &self,
        request: Request<UpsertProjectRequest>,
    ) -> Result<Response<UpsertProjectResponse>, Status> {
        let remote_addr = format_remote_addr(request.remote_addr());
        let req = request.into_inner();
        let project = self
            .usecases
            .upsert_project(&req.name, &req.path)
            .await
            .map_err(status_from_app_error)?;
        log::info!(
            "upsert_project remote_addr={remote_addr} name={} path={}",
            project.name,
            project.path
        );
        Ok(Response::new(UpsertProjectResponse {
            project: Some(project_record_to_response(&project)),
        }))
    }

    async fn get_project(
        &self,
        request: Request<GetProjectRequest>,
    ) -> Result<Response<GetProjectResponse>, Status> {
        let remote_addr = format_remote_addr(request.remote_addr());
        let req = request.into_inner();
        let project = self
            .usecases
            .get_project_by_name(&req.name)
            .await
            .map_err(status_from_app_error)?;
        log::info!(
            "get_project remote_addr={remote_addr} name={} path={}",
            project.name,
            project.path
        );
        Ok(Response::new(GetProjectResponse {
            project: Some(project_record_to_response(&project)),
        }))
    }

    async fn list_projects(
        &self,
        request: Request<ListProjectsRequest>,
    ) -> Result<Response<ListProjectsResponse>, Status> {
        let remote_addr = format_remote_addr(request.remote_addr());
        let _ = request.into_inner();
        let projects = self
            .usecases
            .list_projects()
            .await
            .map_err(status_from_app_error)?;
        let projects = projects
            .iter()
            .map(project_record_to_response)
            .collect::<Vec<_>>();
        log::info!(
            "list_projects remote_addr={remote_addr} count={}",
            projects.len()
        );
        Ok(Response::new(ListProjectsResponse { projects }))
    }

    async fn delete_project(
        &self,
        request: Request<DeleteProjectRequest>,
    ) -> Result<Response<DeleteProjectResponse>, Status> {
        let remote_addr = format_remote_addr(request.remote_addr());
        let name = request.into_inner().name.trim().to_string();
        let deleted = self
            .usecases
            .delete_project(&name)
            .await
            .map_err(status_from_app_error)?;
        log::info!("delete_project remote_addr={remote_addr} name={name} deleted={deleted}");
        Ok(Response::new(DeleteProjectResponse { deleted }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proto::add_cluster_init;

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
}
