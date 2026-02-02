// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

use clap::FromArgMatches;
use orbit::adapters;
use orbit::adapters::cli::{Cli, Cmd};
use orbit::app::dispatcher::Dispatcher;
use orbit::app::ports::{ConfigPort, FilesystemPort, InteractionPort, NetworkPort, OutputPort};
use orbit::app::{AppContext, UiMode};
use std::sync::Arc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut cmd = adapters::cli::cli_command();
    let matches = cmd.get_matches();
    let cli = Cli::from_arg_matches(&matches).unwrap_or_else(|err| err.exit());
    if let Cmd::Completions(args) = &cli.cmd {
        adapters::cli::generate_shell_completions(args.shell);
        return Ok(());
    }

    let ui_mode = if cli.non_interactive {
        UiMode::NonInteractive
    } else {
        UiMode::Interactive
    };

    let config_path = cli.config.clone();
    let command = adapters::cli::command_from_cli(cli, &matches);

    let config_adapter = adapters::config::ConfigAdapter;
    let daemon_endpoint = config_adapter
        .daemon_endpoint(config_path)
        .map_err(|err| anyhow::anyhow!(err.message))?;

    let orbitd = Arc::new(adapters::grpc::GrpcOrbitdPort::new(daemon_endpoint));
    let output: Arc<dyn OutputPort> = match ui_mode {
        UiMode::Interactive => Arc::new(adapters::terminal::TerminalOutput::new()),
        UiMode::NonInteractive => Arc::new(adapters::json::JsonOutput::new()),
    };
    let interaction: Arc<dyn InteractionPort> = match ui_mode {
        UiMode::Interactive => Arc::new(adapters::terminal::TerminalInteraction::new()),
        UiMode::NonInteractive => Arc::new(adapters::json::NonInteractiveInteraction::new()),
    };
    let fs: Arc<dyn FilesystemPort> = Arc::new(adapters::fs::StdFilesystem);
    let network: Arc<dyn NetworkPort> = Arc::new(adapters::network::StdNetwork);

    let ctx = AppContext {
        ui_mode,
        orbitd,
        interaction,
        output,
        fs,
        network,
    };
    let dispatcher = Dispatcher::new(ctx);
    let exit_code = dispatcher
        .dispatch(command)
        .await
        .map_err(|err| anyhow::anyhow!(err.message))?;
    if exit_code != 0 {
        std::process::exit(exit_code);
    }
    Ok(())
}
