use anyhow::bail;
use clap::{ArgGroup, Args, Parser, Subcommand};
use proto::agent_client::AgentClient;
use proto::{
    AddClusterInit, AddClusterRequest, ListClustersRequest, MfaAnswer, PingReply, PingRequest,
    StreamEvent, SubmitRequest, SubmitRequestInit, add_cluster_init, add_cluster_request,
    stream_event,
};
use std::io::Write;
use std::path::PathBuf;
use tokio::io;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use tokio::time::{Duration, timeout};
use tokio_stream::{Stream, StreamExt, wrappers::ReceiverStream};
use tonic::{Request, Response, Status, transport::Channel};
use tonic_types::StatusExt;

mod hpcfile;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    Ping,
    Ls,
    Submit {
        hostid: String,
        local_path: String,
        remote_path: String,
    },
    AddCluster(AddClusterArgs),
    ListClusters,
    Init(InitProjectArgs),
}

#[derive(Args, Debug)]
struct InitProjectArgs {
    // path to project to be initialized
    projectpath: PathBuf,

    // force init project: ignore directory if exists, make sure that all directories within the
    // path
    #[arg(long, short)]
    force: bool,
}

#[derive(Args, Debug)]
#[command(
    group(
        ArgGroup::new("addcluster")
            .required(true)      // at least one is required…
            .multiple(false)     // …and they are mutually exclusive
            .args(&["hostname", "ip"])
    )
)]
struct AddClusterArgs {
    #[arg(long, value_name = "HOSTNAME")]
    hostname: Option<String>,

    /// Use a remote URL as input
    #[arg(long, value_name = "IP")]
    ip: Option<String>,

    #[arg(long)]
    username: String,

    #[arg(long)]
    hostid: String,

    #[arg(long, default_value_t = 22)]
    port: u32,

    #[arg(long, default_value = "~/.ssh/id_ed25519")]
    identity_path: String,
}

async fn send_ping(client: &mut AgentClient<Channel>) -> anyhow::Result<()> {
    let ping_request = PingRequest {
        message: "ping".into(),
    };
    let response = match timeout(Duration::from_secs(1), client.ping(ping_request)).await {
        Ok(res) => match res {
            Ok(response) => response,
            Err(status) => match status.code() {
                tonic::Code::InvalidArgument => {
                    bail!("invalid argument: {}", status.message());
                }
                tonic::Code::Cancelled => {
                    bail!("operation was canceled:{}", status.message());
                }
                _ => {
                    bail!("error occured: {}", status);
                }
            },
        },
        Err(elapsed) => bail!("Cancelled request after {elapsed} seconds"),
    };
    let message = response.get_ref().to_owned().message;
    match message.as_str() {
        "pong" => Ok(()),
        v => bail!("invalid response from server: expected 'pong', got '{v}'"),
    }
}
async fn send_list_clusters(client: &mut AgentClient<Channel>, filter: &str) -> anyhow::Result<()> {
    let list_clusters_request = ListClustersRequest {
        filter: filter.to_string(),
    };
    let response = match timeout(
        Duration::from_secs(1),
        client.list_clusters(list_clusters_request),
    )
    .await
    {
        Ok(Ok(res)) => res.into_inner(),
        Ok(Err(status)) => match status.code() {
            tonic::Code::InvalidArgument => {
                bail!("invalid argument: '{}'", status.message())
            }
            tonic::Code::Internal => {
                bail!("internal error: '{}'", status.message())
            }
            _ => {
                bail!(
                    "error encountered: {} - '{}'",
                    status.code(),
                    status.message()
                )
            }
        },
        Err(e) => {
            bail!("operation timed out: {}", e)
        }
    };
    // TODO: go through list of clusters to determine the lengths of fields
    println!(
        "{:<12} {:<16} {:<20} {:<4} {:<12}",
        "username", "hostid", "address", "port", "status"
    );

    for item in response.clusters.iter() {
        let host_str = match item.host {
            Some(ref v) => match v {
                proto::list_clusters_unit_response::Host::Hostname(s) => s,
                proto::list_clusters_unit_response::Host::Ipaddr(s) => s,
            },
            None => "<unknown>",
        };

        let connected_str = match item.connected {
            true => "connected",
            false => "disconnected",
        };
        println!(
            "{:<12} {:<16} {:<20} {:<4} {:<12}",
            item.username, item.hostid, host_str, item.port, connected_str
        );
    }

    Ok(())
}
async fn send_ls(client: &mut AgentClient<Channel>) -> anyhow::Result<()> {
    // outgoing stream client -> server with MFA answers
    let (tx_ans, rx_ans) = mpsc::channel::<MfaAnswer>(16);
    let outbound = ReceiverStream::new(rx_ans);

    // Start LS RPC
    let response = client.ls(Request::new(outbound)).await?;
    let mut inbound = response.into_inner();
    let mut exit_code: Option<i32> = None;
    while let Some(item) = inbound.next().await {
        match item {
            Ok(StreamEvent { event: Some(ev) }) => match ev {
                stream_event::Event::Stdout(bytes) => {
                    write_all(&mut std::io::stdout(), &bytes);
                }
                stream_event::Event::Stderr(bytes) => {
                    write_all(&mut std::io::stderr(), &bytes)?;
                }
                stream_event::Event::ExitCode(code) => {
                    exit_code = Some(code);
                    break;
                }
                stream_event::Event::Mfa(mfa) => {
                    // Prompt for all answers in this round
                    eprintln!();
                    if !mfa.name.is_empty() {
                        eprintln!("MFA: {}", mfa.name);
                    }
                    if !mfa.instructions.is_empty() {
                        eprintln!("{}", mfa.instructions);
                    }

                    let mut responses = Vec::with_capacity(mfa.prompts.len());
                    for p in &mfa.prompts {
                        let ans = prompt_value(&p.text, p.echo).await?;
                        responses.push(ans);
                    }

                    // Send the answers back
                    if tx_ans.send(MfaAnswer { responses }).await.is_err() {
                        eprintln!("server closed while sending MFA answers");
                        break;
                    }
                }
                stream_event::Event::Error(err) => {
                    // "command not allowed" goes here
                    // "SSH connect failed also goes here"
                    eprintln!("server error: {err}");
                    exit_code = Some(1);
                    break;
                }
            },
            Ok(StreamEvent { event: None }) => log::info!("received empty event"),
            Err(status) => {
                eprintln!("stream error: {}", status);
                exit_code = Some(1);
                break;
            }
        }
    }
    match exit_code {
        Some(num) => {
            if num != 0 {
                bail!("received exit code {num}");
            }
            return Ok(());
        }
        None => {
            return Ok(());
        }
    }
}

async fn send_submit(
    client: &mut AgentClient<Channel>,
    hostid: &str,
    local_path: &str,
    remote_path: &str,
) -> anyhow::Result<()> {
    // outgoing stream client -> server with MFA answers
    let (tx_ans, rx_ans) = mpsc::channel::<SubmitRequest>(16);
    let outbound = ReceiverStream::new(rx_ans);
    tx_ans
        .send(SubmitRequest {
            msg: Some(proto::submit_request::Msg::Init(proto::SubmitRequestInit {
                local_path: local_path.to_owned(),
                remote_path: remote_path.to_owned(),
                hostid: hostid.to_owned(),
            })),
        })
        .await?;
    // Start LS RPC
    let response = client.submit(Request::new(outbound)).await?;
    let mut inbound = response.into_inner();
    let mut exit_code: Option<i32> = None;
    while let Some(item) = inbound.next().await {
        match item {
            Ok(StreamEvent { event: Some(ev) }) => match ev {
                stream_event::Event::Stdout(bytes) => {
                    write_all(&mut std::io::stdout(), &bytes);
                }
                stream_event::Event::Stderr(bytes) => {
                    write_all(&mut std::io::stderr(), &bytes)?;
                }
                stream_event::Event::ExitCode(code) => {
                    exit_code = Some(code);
                    break;
                }
                stream_event::Event::Mfa(mfa) => {
                    // Prompt for all answers in this round
                    eprintln!();
                    if !mfa.name.is_empty() {
                        eprintln!("MFA: {}", mfa.name);
                    }
                    if !mfa.instructions.is_empty() {
                        eprintln!("{}", mfa.instructions);
                    }

                    let mut responses = Vec::with_capacity(mfa.prompts.len());
                    for p in &mfa.prompts {
                        let ans = prompt_value(&p.text, p.echo).await?;
                        responses.push(ans);
                    }

                    // Send the answers back
                    if tx_ans
                        .send(SubmitRequest {
                            msg: Some(proto::submit_request::Msg::Mfa(MfaAnswer { responses })),
                        })
                        .await
                        .is_err()
                    {
                        eprintln!("server closed while sending MFA answers");
                        break;
                    }
                }
                stream_event::Event::Error(err) => {
                    // "command not allowed" goes here
                    // "SSH connect failed also goes here"
                    eprintln!("server error: {err}");
                    exit_code = Some(1);
                    break;
                }
            },
            Ok(StreamEvent { event: None }) => log::info!("received empty event"),
            Err(status) => {
                eprintln!("stream error: {}", status);
                exit_code = Some(1);
                break;
            }
        }
    }
    match exit_code {
        Some(num) => {
            if num != 0 {
                bail!("on client side: received exit code {num}");
            }
            return Ok(());
        }
        None => {
            return Ok(());
        }
    }
}

async fn send_add_cluster(
    client: &mut AgentClient<Channel>,
    host_id: &str,
    username: &str,
    hostname: &Option<String>,
    ip: &Option<String>,
    identity_path: &str,
    port: u32,
) -> anyhow::Result<()> {
    // outgoing stream client -> server with MFA answers
    let (tx_ans, rx_ans) = mpsc::channel::<AddClusterRequest>(16);
    let outbound = ReceiverStream::new(rx_ans);
    let host: add_cluster_init::Host = match hostname {
        Some(v) => add_cluster_init::Host::Hostname(v.into()),
        None => match ip {
            Some(v) => add_cluster_init::Host::Ipaddr(v.into()),
            None => anyhow::bail!("both hostname and ip address can't be none"),
        },
    };
    let identity_path_expanded = shellexpand::full(identity_path)?;
    let init = AddClusterInit {
        hostid: host_id.to_owned(),
        username: username.to_owned(),
        host: Some(host),
        identity_path: Some(identity_path_expanded.to_string()),
        port: port,
    };
    let acr = AddClusterRequest {
        msg: Some(add_cluster_request::Msg::Init(init)),
    };
    tx_ans.send(acr).await?;
    // Start LS RPC
    let response = client.add_cluster(Request::new(outbound)).await?;
    let mut inbound = response.into_inner();
    let mut exit_code: Option<i32> = None;
    // TODO: put MFA-handling logic in a separate function
    while let Some(item) = inbound.next().await {
        match item {
            Ok(StreamEvent { event: Some(ev) }) => match ev {
                stream_event::Event::Stdout(bytes) => {
                    write_all(&mut std::io::stdout(), &bytes);
                }
                stream_event::Event::Stderr(bytes) => {
                    write_all(&mut std::io::stderr(), &bytes)?;
                }
                stream_event::Event::ExitCode(code) => {
                    exit_code = Some(code);
                    break;
                }
                stream_event::Event::Mfa(mfa) => {
                    // Prompt for all answers in this round
                    eprintln!();
                    if !mfa.name.is_empty() {
                        eprintln!("MFA: {}", mfa.name);
                    }
                    if !mfa.instructions.is_empty() {
                        eprintln!("{}", mfa.instructions);
                    }

                    let mut responses = Vec::with_capacity(mfa.prompts.len());
                    for p in &mfa.prompts {
                        let ans = prompt_value(&p.text, p.echo).await?;
                        responses.push(ans);
                    }

                    // Send the answers back
                    if tx_ans
                        .send(AddClusterRequest {
                            msg: Some(proto::add_cluster_request::Msg::Mfa(MfaAnswer {
                                responses,
                            })),
                        })
                        .await
                        .is_err()
                    {
                        eprintln!("server closed while sending MFA answers");
                        break;
                    }
                }
                stream_event::Event::Error(err) => {
                    // "command not allowed" goes here
                    // "SSH connect failed also goes here"
                    eprintln!("server error: {err}");
                    exit_code = Some(1);
                    break;
                }
            },
            Ok(StreamEvent { event: None }) => log::info!("received empty event"),
            Err(status) => {
                eprintln!("stream error: {}", status);
                exit_code = Some(1);
                break;
            }
        }
    }
    match exit_code {
        Some(num) => {
            if num != 0 {
                bail!("on client side: received exit code {num}");
            }
            return Ok(());
        }
        None => {
            return Ok(());
        }
    }
}

async fn run_init_project(project_path: PathBuf, force: bool) -> anyhow::Result<()> {
    if project_path.exists() {
        if !project_path.is_dir() {
            bail!("{} is not a directory", project_path.to_string_lossy());
        }
        if !force {
            bail!(
                "{} exists - use --force to overwrite that instead",
                project_path.to_string_lossy()
            )
        }
    }

    if !project_path.exists() {
        if force {
            tokio::fs::create_dir_all(&project_path).await?;
        } else {
            tokio::fs::create_dir(&project_path).await?;
        }
    }

    let hpcfile_path = project_path.join("Hpcfile");
    if hpcfile_path.exists() & !force {
        bail!(
            "{} exists, can't init it - use --force if you want overwrite it",
            hpcfile_path.to_string_lossy()
        );
    }
    let hpcfile_config = hpcfile::Hpcfile::default();
    let hpcfile_content = toml::to_string(&hpcfile_config)?;

    tokio::fs::write(&hpcfile_path, &hpcfile_content.into_bytes()).await?;

    Ok(())
}

fn write_all<W: Write>(w: &mut W, buf: &[u8]) -> anyhow::Result<()> {
    w.write_all(buf)?;
    w.flush()?;
    Ok(())
}

async fn prompt_value(prompt: &str, echo: bool) -> anyhow::Result<String> {
    let prompt = prompt.to_string();
    if echo {
        tokio::task::spawn_blocking(move || -> anyhow::Result<String> {
            print!("{}", prompt);
            std::io::stdout().flush()?;
            let mut s = String::new();
            std::io::stdin().read_line(&mut s)?;
            // Trim common line endings
            while s.ends_with('\n') || s.ends_with('\r') {
                s.pop();
            }
            Ok(s)
        })
        .await?
    } else {
        bail!("Method not supported")
        /*
        tokio::task::spawn_blocking(move || -> Result<String> {
            let s = rpassword::prompt_password(prompt)?;
            Ok(s)
        })
        .await?
        */
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut client = AgentClient::connect("http://127.0.0.1:50056").await?;
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Ping => match send_ping(&mut client).await {
            Ok(()) => println!("pong"),
            Err(e) => bail!(e),
        },
        Cmd::Ls => send_ls(&mut client).await?,
        Cmd::Submit {
            hostid,
            local_path,
            remote_path,
        } => send_submit(&mut client, &hostid, &local_path, &remote_path).await?,
        Cmd::AddCluster(add_cluster_args) => {
            send_add_cluster(
                &mut client,
                &add_cluster_args.hostid,
                &add_cluster_args.username,
                &add_cluster_args.hostname,
                &add_cluster_args.ip,
                &add_cluster_args.identity_path,
                add_cluster_args.port,
            )
            .await?
        }
        Cmd::ListClusters => {
            send_list_clusters(&mut client, "").await?;
        }
        Cmd::Init(args) => run_init_project(args.projectpath, args.force).await?,
    }
    Ok(())
}
