use crate::ssh::SessionManager;
use crate::ssh::receiver_to_stream;
use proto::agent_server::{Agent, AgentServer};
use proto::{
    MfaAnswer, MfaPrompt, PingReply, PingRequest, StreamEvent, SubmitRequest, agent_client,
    stream_event,
};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_stream::{Stream, StreamExt};
use tonic::Status;
mod slurm;
use crate::state::db::HostStore;
use std::pin::Pin;
type OutStream = Pin<Box<dyn Stream<Item = Result<StreamEvent, Status>> + Send + Sync + 'static>>;

#[derive(Clone)]
pub struct AgentSvc {
    mgr: Arc<SessionManager>,
    hs: HostStore,
}

impl AgentSvc {
    pub fn new(mgr: Arc<SessionManager>, hs: HostStore) -> Self {
        AgentSvc { mgr: mgr, hs: hs }
    }
    async fn run_command(
        &self,
        command: &str,
        req: tonic::Request<tonic::Streaming<MfaAnswer>>,
    ) -> Result<tonic::Response<OutStream>, Status> {
        // Outbound stream server -> client
        let (evt_tx, evt_rx) = tokio::sync::mpsc::channel::<Result<StreamEvent, Status>>(64);

        // Setting up inbound stream (client -> server) carrying MFA answers from client to mfa_tx
        let mut inbound = req.into_inner();
        let (mfa_tx, mfa_rx) = tokio::sync::mpsc::channel::<MfaAnswer>(16);
        tokio::spawn(async move {
            while let Some(item) = inbound.next().await {
                match item {
                    Ok(ans) => {
                        if mfa_tx.send(ans).await.is_err() {
                            break;
                        }
                    }
                    Err(_) => {
                        break;
                    }
                }
            }
        });

        // execute command over SSH
        let mgr = self.mgr.clone();
        let cmd = command.to_string();
        tokio::spawn(async move {
            if let Err(err) = mgr.exec(&cmd, evt_tx.clone(), mfa_rx).await {
                let _ = evt_tx
                    .send(Ok(StreamEvent {
                        event: Some(stream_event::Event::Error(err.to_string())),
                    }))
                    .await;
            }
        });
        let out: OutStream = Box::pin(receiver_to_stream(evt_rx));
        Ok(tonic::Response::new(out))
    }

    async fn submit_full(
        &self,
        request: tonic::Request<tonic::Streaming<SubmitRequest>>,
    ) -> Result<tonic::Response<OutStream>, Status> {
        log::debug!("submit request initiated");
        // Setting up inbound stream (client -> server) carrying data or MFA answers from client to mfa_tx
        let mut inbound = request.into_inner();

        // Spawning a thread to handle the incoming data
        let init = inbound
            .message()
            .await
            .map_err(|e| Status::unknown(format!("read error: {e}")))?
            .ok_or_else(|| Status::invalid_argument("stream closed before init"))?;
        let (local_path, remote_path) = match init.msg {
            Some(proto::submit_request::Msg::Init(i)) => (i.local_path, i.remote_path),
            _ => return Err(Status::invalid_argument("first message must be init(path)")),
        };
        log::debug!("transfering data from {} to {}", &local_path, &remote_path);
        let (evt_tx, evt_rx) = tokio::sync::mpsc::channel::<Result<StreamEvent, Status>>(64);

        // Pipe the remaining client messages (if any) into MFA answers
        let (mfa_tx, mfa_rx) = tokio::sync::mpsc::channel::<MfaAnswer>(16);
        tokio::spawn(async move {
            while let Ok(Some(item)) = inbound.message().await {
                if let Some(proto::submit_request::Msg::Mfa(ans)) = item.msg {
                    if mfa_tx.send(ans).await.is_err() {
                        break;
                    }
                }
            }
        });

        let mgr = self.mgr.clone();
        tokio::spawn(async move {
            //if let Err(err) = mgr.sync_dir(&local_path, evt_tx.clone(), mfa_rx).await {
            if let Err(err) = mgr
                .sync_dir(
                    &local_path,
                    &remote_path,
                    Some(1024 * 1024),
                    None,
                    &evt_tx.clone(),
                    mfa_rx,
                )
                .await
            {
                let _ = evt_tx
                    .send(Ok(StreamEvent {
                        event: Some(stream_event::Event::Error(err.to_string())),
                    }))
                    .await;
            }
        });

        let out: OutStream = Box::pin(receiver_to_stream(evt_rx));
        Ok(tonic::Response::new(out))
    }
    async fn list_partitions(&self) -> anyhow::Result<Vec<String>> {
        let res: Vec<String> = Vec::new();
        Ok(res)
    }
}

#[tonic::async_trait]
impl Agent for AgentSvc {
    type LsStream = OutStream;
    type SubmitStream = OutStream;
    async fn ping(
        &self,
        request: tonic::Request<PingRequest>,
    ) -> Result<tonic::Response<PingReply>, Status> {
        let req = request.into_inner();
        match req.message.trim() {
            "ping" => {
                return Ok(tonic::Response::new(PingReply {
                    message: "pong".into(),
                }));
            }
            m => {
                return Err(Status::invalid_argument(format!(
                    "expected message 'ping', got '{}'",
                    m
                )));
            }
        }
    }
    async fn ls(
        &self,
        request: tonic::Request<tonic::Streaming<MfaAnswer>>,
    ) -> Result<tonic::Response<Self::LsStream>, Status> {
        self.run_command("ls", request).await
    }

    async fn submit(
        &self,
        request: tonic::Request<tonic::Streaming<SubmitRequest>>,
    ) -> Result<tonic::Response<Self::SubmitStream>, Status> {
        self.submit_full(request).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
}
