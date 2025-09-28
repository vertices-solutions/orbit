use anyhow::Context;
use clap::Parser;
use log::LevelFilter;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_stream::{Stream, StreamExt};
use tonic::Status;
use tonic::transport::Server;
mod agent;
mod ssh;
mod state;
use ssh::{SessionManager, SshParams};

use proto::agent_server::AgentServer;

#[derive(Parser)]
#[command(version,about,long_about = None)]
struct Opts {
    #[arg(short, long)]
    remote_server: String,

    #[arg(short, long)]
    username: String,

    #[arg(short, long)]
    identity_path: String,

    #[arg(short, long)]
    database_path: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::builder()
        .filter_level(LevelFilter::Debug)
        .init();

    let opts = Opts::parse();
    let db = state::db::HostStore::open(opts.database_path).await?;
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
    let sm = Arc::new(SessionManager::new(ssh_params));
    let svc = agent::AgentSvc::new(sm, db);
    println!("server listening on {}", server_addr);
    Server::builder()
        .add_service(AgentServer::new(svc))
        .serve(server_addr)
        .await?;
    Ok(())
}
