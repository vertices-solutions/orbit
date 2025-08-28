use anyhow::Context;
use clap::Parser;
use proto::agent_server::{Agent, AgentServer};
use proto::{MfaAnswer, PingReply, PingRequest, StreamEvent};
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use tokio_stream::Stream;
use tonic::Status;
mod ssh;
use ssh::{SessionManager, SshParams};
#[derive(Parser)]
#[command(version,about,long_about = None)]
struct Opts {
    #[arg(short, long)]
    remote_server: String,

    #[arg(short, long)]
    username: String,

    #[arg(short, long)]
    identity_path: String,
}

type OutStream = Pin<Box<dyn Stream<Item = Result<StreamEvent, Status>> + Send + Sync + 'static>>;

#[derive(Clone)]
struct AgentSvc {
    mgr: Arc<SessionManager>,
}
#[tonic::async_trait]
impl Agent for AgentSvc {
    type LsStream = OutStream;
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
    }
}
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opts = Opts::parse();
    let server_addr: SocketAddr = "127.0.0.1:50056".parse()?;
    let remote_server: SocketAddr = opts
        .remote_server
        .parse()
        .context("failed to parse remote_server address")?;

    let ssh_params = SshParams {
        addr: remote_server,
        username: opts.username,
        identity_path: Some(opts.identity_path),
        keepalive_secs: 60,
        ki_submethods: None,
    };
    let sm = SessionManager::new(ssh_params);

    println!("hello world!");
    Ok(())
}
